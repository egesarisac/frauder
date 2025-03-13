from abc import ABC, abstractmethod
import structlog
from .models import Transaction

logger = structlog.get_logger()

class FraudRule(ABC):
    @abstractmethod
    def check(self, transaction: Transaction) -> bool:
        pass

class RuleEvaluator:
    """Service for evaluating fraud detection rules."""
    
    def __init__(self, metric_service):
        self.metric_service = metric_service
    
    def compare(self, value, threshold, operator):
        """Compare value with threshold using the specified operator."""
        # Convert both values to float for numeric comparison
        try:
            value = float(value)
            threshold = float(threshold)
        except (ValueError, TypeError) as e:
            # If conversion fails, try string comparison
            value = str(value)
            threshold = str(threshold)

        logger.info(
            "comparing values",
            value=value,
            threshold=threshold,
            value_type=type(value),
            threshold_type=type(threshold)
        )

        ops = {
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            "-": lambda a, b: a - b >= b,  # Added: Check if a is greater than 2*b (a-b >= b)
        }
        return ops[operator](value, threshold)
    
    def evaluate_rule(self, rule, transaction):
        """Evaluate a single rule with multiple conditions."""
        condition_results = []
        
        for condition in rule.get("conditions", []):
            logger.info("Evaluating condition", condition=condition)
            
            if condition["type"] == "metric":
                # For metric-based conditions
                metric_config = condition["metric"]
                field = metric_config.get("field", "amount")
                metric_type = metric_config.get("type")
                
                # Get historical metric value - pass the entire rule to enable direct condition filtering
                historical_value = self.metric_service.calculate_metric(metric_config, transaction, rule=rule)
                
                # Get current transaction value
                current_value = transaction.get(field, 0)
                
                # Calculate threshold based on historical value
                threshold = condition["threshold"]
                operator = condition["operator"]
                
                # Special handling for difference-based comparison
                if operator == "-" and condition["threshold_type"] == "absolute":
                    # For condition like "current_value >= historical_value + threshold"
                    # We compare if (current_value - historical_value) >= threshold
                    value_to_compare = current_value
                    result = (current_value - historical_value) >= threshold
                    logger.info(
                        "Difference-based comparison",
                        current_value=current_value,
                        historical_value=historical_value,
                        difference=current_value - historical_value,
                        threshold=threshold,
                        result=result
                    )
                else:
                    # Regular percentage or absolute comparison
                    if condition["threshold_type"] == "percent":
                        threshold = historical_value * (threshold / 100)

                    logger.info("metric details", metric_type=metric_type, historical_value=historical_value)
                    
                    # For different metric types, determine what value to compare against threshold
                    if metric_type == "count":
                        # For count, we compare the historical count with the threshold
                        value_to_compare = historical_value
                    elif metric_type == "max":
                        # For max, we compare the historical max with the threshold
                        value_to_compare = historical_value
                    elif metric_type == "min":
                        # For min, we compare the historical min with the threshold
                        value_to_compare = historical_value
                    else:
                        # For other metrics (avg, sum), we compare the current value with the threshold
                        value_to_compare = current_value
                    
                    logger.info(
                        "Metric comparison",
                        historical_value=historical_value,
                        current_value=current_value,
                        value_to_compare=value_to_compare,
                        threshold=threshold,
                        threshold_type=condition["threshold_type"],
                        metric_type=metric_type,
                        operator=operator
                    )
                    
                    result = self.compare(value_to_compare, threshold, operator)
                
                condition_results.append(result)
                logger.info(
                    "Condition result",
                    condition_type=condition["type"],
                    current_value=current_value,
                    operator=operator,
                    result=result,
                    threshold=condition.get("threshold", "N/A")
                )
                continue
            else:
                # For direct comparisons
                field = condition["field"]
                current_value = transaction.get(field, 0)
                threshold = condition["value"]
                
                # Get field type from Transaction model
                field_type = Transaction.__annotations__.get(field)
                
                # Convert values based on field type
                try:
                    if field_type == int:
                        current_value = int(current_value)
                        threshold = int(threshold)
                    elif field_type == float:
                        current_value = float(current_value)
                        threshold = float(threshold)
                    elif field_type == str:
                        current_value = str(current_value)
                        threshold = str(threshold)
                    # Add more type conversions as needed
                except (ValueError, TypeError) as e:
                    logger.error(f"Error converting values for comparison: {e}")
                    return False
                
                result = self.compare(current_value, threshold, condition["operator"])
                condition_results.append(result)
                
                logger.info(
                    "Condition result",
                    condition_type=condition["type"],
                    current_value=transaction.get(condition.get("field", "amount"), 0),
                    operator=condition.get("operator", ""),
                    result=result,
                    threshold=condition.get("threshold", condition.get("value", "N/A"))
                )
        
        # Apply logical operator
        logical_operator = rule.get("logical_operator", "AND")
        final_result = all(condition_results) if logical_operator == "AND" else any(condition_results)
        logger.info(
            "Rule evaluation result",
            rule_id=rule.get("rule_id"),
            rule_name=rule.get("name"),
            logical_operator=logical_operator,
            conditions_results=condition_results,
            final_result=final_result
        )
        
        return final_result

class DynamicRule(FraudRule):
    def __init__(self, rule_evaluator, rule_data):
        self.rule_evaluator = rule_evaluator
        self.rule_data = rule_data

    def check(self, transaction: Transaction) -> bool:
        return self.rule_evaluator.evaluate_rule(self.rule_data, transaction.__dict__)