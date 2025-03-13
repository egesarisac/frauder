import structlog
from datetime import datetime
from .models import Transaction

logger = structlog.get_logger()

class MetricService:
    """Service for calculating and evaluating metrics used in fraud detection."""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
    
    def get_metric_key(self, rule_metric, transaction, direct_condition_suffix=None):
        """Generate Redis key for storing metric data.
        
        Args:
            rule_metric: Metric configuration
            transaction: Transaction data
            direct_condition_suffix: Optional suffix for direct condition filtering
        """
        group_fields = rule_metric.get("group_by", [])
        group_values = [str(transaction.get(field, "")) for field in group_fields]
        group_value = ":".join(group_values)
        
        # For count metrics, we don't need a field - just count transactions
        if rule_metric['type'] == 'count':
            base_key = f"metric:count:group:{group_value}"
        else:
            field = rule_metric.get('field', 'amount')
            base_key = f"metric:{rule_metric['type']}:{field}:group:{group_value}"
            
        # If we have a direct condition suffix, add it to the key
        if direct_condition_suffix:
            return f"{base_key}:{direct_condition_suffix}"
        return base_key

    def parse_window(self, window_str):
        """Parse window string (e.g. '2 hours' or '30 days') into seconds."""
        try:
            value, unit = window_str.split()
            value = int(value)
            if unit == 'hours':
                return value * 3600
            elif unit == 'days':
                return value * 86400
            else:
                logger.error(f"Invalid window unit: {unit}")
                return None
        except (ValueError, AttributeError) as e:
            logger.error(f"Error parsing window string: {window_str}, error: {e}")
            return None

    def calculate_metric(self, rule_metric, transaction, rule=None):
        """Calculate metric (avg/sum/count) for a rule's configuration.
        
        Args:
            rule_metric: The metric configuration
            transaction: The transaction data
            rule: Optional parent rule for handling direct conditions
        """
        # Check if we need to include direct conditions in the key (for AND rules)
        direct_condition_suffix = None
        
        if rule and rule.get("logical_operator") == "AND":
            direct_fields = []
            for condition in rule.get("conditions", []):
                if condition["type"] == "direct":
                    field = condition["field"]
                    value = condition["value"]
                    operator = condition["operator"]
                    # Ensure consistent formatting between storage and retrieval
                    if isinstance(value, float) and value.is_integer():
                        value = int(value)
                    direct_fields.append(f"{field}_{operator}_{value}")
            
            if direct_fields:
                direct_condition_suffix = "direct_" + "_".join(direct_fields)
        
        metric_key = self.get_metric_key(rule_metric, transaction, direct_condition_suffix)
        
        # Check if there are any keys in Redis that start with this prefix (debug)
        all_keys = self.redis_client.keys(f"{metric_key}*")
        if all_keys:
            logger.info(
                "Found related Redis keys",
                metric_key=metric_key,
                related_keys=all_keys
            )
        field = rule_metric.get('field', 'amount')
        current_value = transaction.get(field, 0)
        
        # Parse window string into seconds
        window_seconds = self.parse_window(rule_metric.get('window', '2 hours'))
        if window_seconds is None:
            return 0
        
        # Use transaction timestamp for window calculation
        transaction_time = transaction.get('timestamp', datetime.now().timestamp())
        window_start = transaction_time - window_seconds if window_seconds else 0
        
        logger.info(
            "Redis window calculation",
            transaction_time=transaction_time,
            window_start=window_start,
            window_seconds=window_seconds
        )
        
        # Get historical values (excluding current transaction)
        # Check if the key exists first
        key_exists = self.redis_client.exists(metric_key)
        logger.info(
            "Redis key check",
            metric_key=metric_key,
            exists=bool(key_exists)
        )
        
        values = self.redis_client.zrangebyscore(
            metric_key,
            min=window_start,
            max=transaction_time,
            withscores=False
        )
        
        if not values:
            logger.info(
                "No values found in Redis",
                metric_key=metric_key,
                min=window_start,
                max=transaction_time
            )
        
        # For count metrics, we just count the number of transactions
        # For other metrics, parse field values based on Transaction model field types
        fields = []
        metric_type = rule_metric["type"]
        field = rule_metric.get('field')
        if metric_type == "count":
            fields = values  # Just count the number of entries
        else:
            # Get field type from Transaction model
            field_type = Transaction.__annotations__.get(field)
            
            for val in values:
                if isinstance(val, bytes):
                    val = val.decode()
                if isinstance(val, str) and ':' in val:
                    # Format: "id:field_value:timestamp"
                    field_value = val.split(':', 2)[1]
                else:
                    field_value = val
                
                # Convert value based on field type
                try:
                    if field_type == int:
                        field_value = int(field_value)
                    elif field_type == float:
                        field_value = float(field_value)
                    elif field_type == str:
                        field_value = str(field_value)
                    # Add more type conversions as needed
                except (ValueError, TypeError) as e:
                    logger.error(f"Error converting field value: {e}")
                    continue
                
                fields.append(field_value)
        logger.info(
            "Historical values",
            fields=fields,
            current_value=current_value,
            metric_type=rule_metric["type"],
            window=rule_metric.get('window')
        )
        # Calculate metric based on type (using only historical values)
        result = 0
        
        # Convert fields to the correct type before calculation
        if metric_type != "count":
            field = rule_metric.get('field')
            field_type = Transaction.__annotations__.get(field, int)  # default to int
            
            # Convert all fields to the correct type
            typed_fields = []
            for val in fields:
                try:
                    if field_type == int:
                        typed_fields.append(int(val))
                    elif field_type == float:
                        typed_fields.append(float(val))
                    elif field_type == str:
                        typed_fields.append(str(val))
                except (ValueError, TypeError) as e:
                    logger.error(f"Error converting field value {val} to {field_type}: {e}")
                    continue
            fields = typed_fields
            
            # Also convert current_value to the correct type
            try:
                if field_type == int:
                    current_value = int(current_value)
                elif field_type == float:
                    current_value = float(current_value)
                elif field_type == str:
                    current_value = str(current_value)
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting current_value {current_value} to {field_type}: {e}")
                current_value = 0
        
        if metric_type == "count":
            # For count, we want just the number of historical transactions
            # The comparison will be: if historical_count >= threshold
            # So for a rule "more than 3 transactions", it will trigger on the 4th transaction
            result = len(fields)
        elif metric_type == "average":
            # For average, we should only use historical values (excluding current transaction)
            # Remove current transaction value from fields if it exists
            current_str = str(current_value)
            historical_fields = [f for f in fields if str(f) != current_str]
            result = sum(historical_fields) / len(historical_fields) if historical_fields else current_value
            logger.info(
                "Average calculation",
                historical_fields=historical_fields,
                current_value=current_value,
                result=result
            )
        elif metric_type == "sum":
            result = sum(fields)
        elif metric_type == "max":
            result = max(fields) if fields else current_value
        elif metric_type == "min":
            result = min(fields) if fields else current_value
        else:
            raise ValueError(f"Unknown metric type: {metric_type}")
            
        logger.info(
            "Calculated metric",
            metric_type=metric_type,
            result=result,
            num_historical_values=len(fields),
            window=rule_metric.get('window')
        )
        return result
    
    def update_metrics(self, rules, transaction):
        """Update Redis with transaction data for metric tracking."""
        # Important: Execute pipeline for each rule to ensure data is stored
        # before it's queried by the rule evaluation
        
        for rule in rules:
            pipeline = self.redis_client.pipeline()  # New pipeline for each rule
            
            # Analyze rule structure to determine direct conditions for AND operations
            direct_conditions = {}
            if rule.get("logical_operator") == "AND":
                # Find all direct conditions in the rule
                for idx, condition in enumerate(rule.get("conditions", [])):
                    if condition["type"] == "direct":
                        direct_conditions[idx] = condition
            
            # Update rule's main metric
            if "metric" in rule:
                self._update_single_metric(pipeline, rule["metric"], transaction, 
                                          rule.get("logical_operator"), direct_conditions)
            
            # Update condition-specific metrics
            for idx, condition in enumerate(rule.get("conditions", [])):
                if "metric" in condition:
                    # For each metric condition, pass all direct conditions that are ANDed with it
                    self._update_single_metric(pipeline, condition["metric"], transaction,
                                              rule.get("logical_operator"), direct_conditions)
            
            # Execute the pipeline immediately to ensure data is stored before rule evaluation
            result = pipeline.execute()
            logger.info(
                "Redis pipeline executed", 
                pipeline_results=len(result),
                rule_id=rule.get("rule_id"))

    def _update_single_metric(self, pipeline, metric_config, transaction, logical_operator=None, direct_conditions=None):
        """Helper to update individual metrics
        
        Args:
            pipeline: Redis pipeline
            metric_config: Metric configuration
            transaction: Transaction data
            logical_operator: Logical operator of the parent rule (AND/OR)
            direct_conditions: Direct conditions from the parent rule
        """
        # Check if direct conditions need to be satisfied (for AND operator)
        direct_condition_suffix = None
        should_store = True
        
        if logical_operator == "AND" and direct_conditions:
            # With AND operator, we only want to store metrics if all direct conditions are met
            direct_fields = []
            for idx, condition in direct_conditions.items():
                field = condition["field"]
                value = condition["value"]
                operator = condition["operator"]
                
                # Get transaction value
                field_value = transaction.get(field, 0)
                
                # Compare using appropriate type
                try:
                    field_value = float(field_value)
                    value = float(value)
                except (ValueError, TypeError):
                    field_value = str(field_value)
                    value = str(value)
                
                # Check if condition is satisfied
                ops = {
                    ">": lambda a, b: a > b,
                    "<": lambda a, b: a < b,
                    ">=": lambda a, b: a >= b,
                    "<=": lambda a, b: a <= b,
                    "==": lambda a, b: a == b,
                    "!=": lambda a, b: a != b,
                }
                
                condition_met = ops[operator](field_value, value)
                if not condition_met:
                    should_store = False
                    break
                
                # Add to direct condition suffix - ensure integer formatting for numeric values
                # Convert float to int if it has no decimal part to maintain consistent formatting
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                direct_fields.append(f"{field}_{operator}_{value}")
            
            if direct_fields:
                direct_condition_suffix = "direct_" + "_".join(direct_fields)
                
        # Only proceed if all direct conditions are met (for AND)
        # or if we don't have AND with direct conditions
        if should_store:
            metric_key = self.get_metric_key(metric_config, transaction, direct_condition_suffix)
            timestamp = transaction.get("timestamp", datetime.now().timestamp())
            transaction_id = transaction.get("transaction_id", 0)
            amount = transaction.get("amount", 0)
            
            # For count metrics, we only need transaction_id and timestamp
            if metric_config['type'] == 'count':
                member = f"{transaction_id}:{int(amount)}:{int(timestamp)}"
                logger.info(
                    "Adding count metric",
                    metric_key=metric_key,
                    transaction_id=transaction_id,
                    amount=amount,
                    timestamp=timestamp,
                    with_direct_conditions=bool(direct_condition_suffix)
                )
            else:
                field = metric_config.get("field", "amount")
                field_value = transaction.get(field, 0)
                member = f"{transaction_id}:{field_value}:{int(timestamp)}"
                logger.info(
                    "Adding value metric",
                    metric_key=metric_key,
                    transaction_id=transaction_id,
                    field=field,
                    value=field_value,
                    timestamp=timestamp,
                    with_direct_conditions=bool(direct_condition_suffix)
                )
            
            # Add to Redis and log the operation
            pipeline.zadd(metric_key, {member: timestamp})
            logger.info(
                "Adding to Redis pipeline",
                metric_key=metric_key,
                member=member,
                timestamp=timestamp
            )
            
            # Parse window string into seconds
            window_seconds = self.parse_window(metric_config.get('window', '2 hours'))
            if window_seconds:
                pipeline.zremrangebyscore(
                    metric_key,
                    0,
                    timestamp - window_seconds
                )
