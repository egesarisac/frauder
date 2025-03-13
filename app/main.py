import os
import time
import signal
import sys
from typing import Dict, Any, Optional
import traceback
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import structlog
from dotenv import load_dotenv
from .cache import RedisClient
from .metric_service import MetricService
from .rule_evaluator import RuleEvaluator
from .fraud_service import FraudService

# Load environment variables
load_dotenv()

logger = structlog.get_logger()

class FraudDetectionService:
    """Main service class for fraud detection.
    
    This service connects to Kafka to consume transaction events,
    processes them through fraud detection rules, and publishes
    fraud alerts to a Kafka topic when suspicious activity is detected.
    """
    
    def __init__(self):
        """Initialize the fraud detection service with Kafka and Redis connections."""
        # Configure from environment variables with fallbacks
        self.transactions_topic = os.getenv('TRANSACTIONS_TOPIC', 'transactions')
        self.fraud_alert_topic = os.getenv('FRAUD_ALERT_TOPIC', 'fraud_alerts')
        self.consumer_group = os.getenv('CONSUMER_GROUP', 'fraud-detection-group')
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'fraud-kafka:29092')
        
        # Setup graceful shutdown handling
        self.running = True
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logger.info("Initializing Fraud Detection Service", 
                   transactions_topic=self.transactions_topic,
                   fraud_alert_topic=self.fraud_alert_topic,
                   kafka_brokers=self.kafka_brokers)
        
        # Initialize components
        self._init_kafka()
        self._init_services()
    
    def _init_kafka(self) -> None:
        """Initialize Kafka consumer and producer connections."""
        try:
            # Create Kafka consumer with error handling
            logger.info("Initializing Kafka consumer", 
                       topic=self.transactions_topic, 
                       brokers=self.kafka_brokers)
            
            self.consumer = KafkaConsumer(
                self.transactions_topic,
                bootstrap_servers=self.kafka_brokers.split(','),
                group_id=self.consumer_group,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=30000,  # 30 seconds
                heartbeat_interval_ms=10000  # 10 seconds
            )
            
            # Create Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_brokers.split(','),
                value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,   # Retry on transient errors
                retry_backoff_ms=500  # Backoff between retries
            )
            
            logger.info("Kafka connections established successfully")
            
        except NoBrokersAvailable as e:
            logger.error("Failed to connect to Kafka brokers", error=str(e))
            sys.exit(1)  # Exit with error
        except KafkaError as e:
            logger.error("Kafka error during initialization", error=str(e))
            sys.exit(1)  # Exit with error
    
    def _init_services(self) -> None:
        """Initialize service components (Redis, metrics, rules)."""
        try:
            # Initialize Redis client
            self.redis_client = RedisClient()
            
            # Initialize metric service
            self.metric_service = MetricService(self.redis_client.redis_client)
            
            # Initialize rule evaluator
            self.rule_evaluator = RuleEvaluator(self.metric_service)
            
            # Initialize fraud service
            self.fraud_service = FraudService(
                self.redis_client, 
                self.metric_service, 
                self.rule_evaluator,
                self.producer
            )
            
            logger.info("All services initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize services", 
                        error=str(e),
                        traceback=traceback.format_exc())
            sys.exit(1)  # Exit with error
    
    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """Handle graceful shutdown on SIGTERM or SIGINT."""
        logger.info("Received shutdown signal, closing connections")
        self.running = False
        
        # Close Kafka connections
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'producer'):
            self.producer.close()
            
        logger.info("Shutdown complete")
        sys.exit(0)

    def process_transaction(self, transaction_data: Dict[str, Any]) -> None:
        """Process a transaction through fraud detection rules.
        
        Args:
            transaction_data: Dictionary containing transaction details
        """
        try:
            logger.info("Processing transaction", 
                       transaction_id=transaction_data.get('transaction_id'),
                       merchant_id=transaction_data.get('merchant_id'))
            
            # Process through fraud service
            self.fraud_service.process_transaction(transaction_data)
            
            logger.info("Transaction processed successfully", 
                       transaction_id=transaction_data.get('transaction_id'))
                       
        except Exception as e:
            logger.error("Error processing transaction", 
                        error=str(e), 
                        transaction_id=transaction_data.get('transaction_id', 'unknown'),
                        traceback=traceback.format_exc())

    def run(self) -> None:
        """Start the main processing loop to consume transactions."""
        logger.info("Starting fraud detection service")
        
        try:
            while self.running:
                # Poll for new messages with timeout
                messages = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, batch in messages.items():
                    for message in batch:
                        if self.running:  # Check before processing each message
                            self.process_transaction(message.value)
                        else:
                            break
                            
        except KafkaError as e:
            logger.error("Kafka error during processing", error=str(e))
            self._handle_shutdown(None, None)  # Graceful shutdown
        except Exception as e:
            logger.error("Unexpected error", 
                        error=str(e),
                        traceback=traceback.format_exc())
            self._handle_shutdown(None, None)  # Graceful shutdown

def main() -> None:
    """Entry point for the fraud detection service."""
    try:
        service = FraudDetectionService()
        service.run()
    except Exception as e:
        logger.critical("Fatal error in fraud detection service", 
                      error=str(e),
                      traceback=traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
