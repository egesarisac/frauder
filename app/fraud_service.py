import os
import structlog
import json
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional
from pydantic import ValidationError
from kafka.errors import KafkaError
from dotenv import load_dotenv
from .models import Transaction

# Load environment variables
load_dotenv()

logger = structlog.get_logger()

class FraudService:
    """Main service for fraud detection processing.
    
    This service orchestrates the fraud detection workflow:
    1. Validates and normalizes incoming transaction data
    2. Loads active fraud detection rules from Redis
    3. Updates metric services with transaction data
    4. Evaluates rules against the transaction
    5. Triggers alerts for any detected fraud
    """
    
    def __init__(self, redis_client, metric_service, rule_evaluator, kafka_producer=None):
        """Initialize the fraud service with required dependencies.
        
        Args:
            redis_client: Redis client for caching and rule storage
            metric_service: Service for calculating and managing metrics
            rule_evaluator: Service for evaluating fraud rules
            kafka_producer: Optional Kafka producer for sending alerts
        """
        self.redis_client = redis_client
        self.metric_service = metric_service
        self.rule_evaluator = rule_evaluator
        self.kafka_producer = kafka_producer
        self.fraud_alert_topic = os.getenv('FRAUD_ALERT_TOPIC', 'fraud_alerts')
        logger.info("Fraud service initialized", fraud_alert_topic=self.fraud_alert_topic)
    
    def process_transaction(self, transaction_data: Dict[str, Any]) -> None:
        """Process a transaction and check for fraud.
        
        Args:
            transaction_data: Dictionary containing transaction details
            
        Raises:
            ValidationError: If transaction data fails validation
            Exception: For any other processing errors
        """
        transaction_id = transaction_data.get('transaction_id', 'unknown')
        
        try:
            # Validate transaction data with Pydantic model
            transaction = Transaction(**transaction_data)
            
            # Load active rules
            active_rules = self.redis_client.load_active_rules()
            if not active_rules:
                logger.warning("No active fraud rules found", transaction_id=transaction_id)
            
            logger.info("Processing transaction with rules", 
                       transaction_id=transaction_id,
                       num_rules=len(active_rules))
            
            # Update metrics for all rules
            self.metric_service.update_metrics(active_rules, transaction_data)
            
            # Track if any rules were triggered
            fraud_detected = False
            
            # Evaluate each rule
            for rule in active_rules:
                rule_id = rule.get('rule_id', 'unknown')
                rule_name = rule.get('name', 'unknown')
                
                try:
                    # Evaluate the rule
                    is_fraud = self.rule_evaluator.evaluate_rule(rule, transaction_data)
                    
                    if is_fraud:
                        fraud_detected = True
                        logger.warning("Fraud detected", 
                                     transaction_id=transaction_id,
                                     rule_id=rule_id,
                                     rule_name=rule_name)
                        
                        # Log the alert in Redis
                        self.redis_client.trigger_alert(transaction_data, rule)
                        
                        # Send alert to Kafka if producer is available
                        if self.kafka_producer:
                            self.send_fraud_alert(transaction, rule)
                            
                except Exception as rule_error:
                    # Log rule evaluation error but continue with other rules
                    logger.error("Error evaluating rule", 
                                error=str(rule_error),
                                rule_id=rule_id,
                                rule_name=rule_name,
                                transaction_id=transaction_id,
                                traceback=traceback.format_exc())
            
            if not fraud_detected:
                logger.info("No fraud detected", transaction_id=transaction_id)
            
            logger.info("Transaction processing completed", 
                       transaction_id=transaction_id,
                       fraud_detected=fraud_detected)
            
        except ValidationError as e:
            # Handle validation errors specifically
            logger.error("Transaction validation failed", 
                        error=str(e),
                        transaction_id=transaction_id,
                        validation_errors=e.errors())
            
        except Exception as e:
            # Catch all other exceptions to prevent service interruption
            logger.error("Error processing transaction", 
                        error=str(e),
                        transaction_id=transaction_id,
                        traceback=traceback.format_exc())
    
    def send_fraud_alert(self, transaction: Transaction, rule: Dict[str, Any]) -> bool:
        """Send fraud alert to Kafka.
        
        Args:
            transaction: Validated transaction object
            rule: Rule configuration that detected the fraud
            
        Returns:
            bool: True if alert was sent successfully, False otherwise
        """
        try:
            # Build alert payload
            alert = {
                'transaction_id': transaction.transaction_id,
                'merchant_id': transaction.merchant_id,
                'amount': transaction.amount,
                'currency': transaction.currency,
                'card_no': transaction.card_no,
                'bank_code': transaction.bank_code,
                'timestamp': transaction.timestamp,
                'rule_id': rule.get('rule_id', 'unknown'),
                'rule_name': rule.get('name', 'unknown'),
                'alert_timestamp': int(datetime.now().timestamp())  # Add detection timestamp
            }
            
            # Send to Kafka with future callback
            future = self.kafka_producer.send(self.fraud_alert_topic, value=alert)
            # Wait for the result with timeout
            record_metadata = future.get(timeout=10)
            
            logger.warning("Fraud alert sent to Kafka", 
                          transaction_id=transaction.transaction_id,
                          rule_id=rule.get("rule_id", "unknown"),
                          rule_name=rule.get("name", "unknown"),
                          topic=record_metadata.topic,
                          partition=record_metadata.partition,
                          offset=record_metadata.offset)
            return True
            
        except KafkaError as e:
            logger.error("Failed to send fraud alert to Kafka", 
                        error=str(e),
                        transaction_id=transaction.transaction_id,
                        rule_id=rule.get('rule_id', 'unknown'))
            return False
            
        except Exception as e:
            logger.error("Unexpected error sending fraud alert", 
                        error=str(e),
                        transaction_id=transaction.transaction_id,
                        traceback=traceback.format_exc())
            return False
