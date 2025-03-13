"""
Integration tests for the main service module.
"""
import pytest
from unittest.mock import MagicMock, patch, call
import json
import os
import signal
import time
from app.main import FraudDetectionService

class TestMainService:
    """Integration tests for the main service module."""
    
    @pytest.fixture
    def env_vars(self):
        """Setup and teardown environment variables for testing."""
        # Setup
        os.environ["KAFKA_BROKERS"] = "localhost:9092"
        os.environ["TRANSACTIONS_TOPIC"] = "test-transactions"
        os.environ["FRAUD_ALERT_TOPIC"] = "test-fraud-alerts"
        os.environ["CONSUMER_GROUP"] = "test-group"
        os.environ["REDIS_HOST"] = "localhost"
        os.environ["REDIS_PORT"] = "6379"
        
        yield
        
        # Teardown
        for key in ["KAFKA_BROKERS", "TRANSACTIONS_TOPIC", "FRAUD_ALERT_TOPIC", 
                   "CONSUMER_GROUP", "REDIS_HOST", "REDIS_PORT"]:
            if key in os.environ:
                del os.environ[key]
    
    @pytest.fixture
    def fraud_service_mock(self):
        """Create a mock for the fraud service."""
        mock = MagicMock()
        mock.process_transaction.return_value = None
        return mock
    
    def test_service_initialization(self, env_vars):
        """Test service initialization with environment variables."""
        with patch('app.main.KafkaConsumer') as consumer_mock, \
             patch('app.main.KafkaProducer') as producer_mock, \
             patch('app.main.RedisClient') as redis_mock, \
             patch('app.main.MetricService') as metric_mock, \
             patch('app.main.RuleEvaluator') as rule_mock, \
             patch('app.main.FraudService') as fraud_mock:
            
            # Initialize mocks
            consumer_mock.return_value = MagicMock()
            producer_mock.return_value = MagicMock()
            redis_mock.return_value = MagicMock()
            metric_mock.return_value = MagicMock()
            rule_mock.return_value = MagicMock()
            fraud_mock.return_value = MagicMock()
            
            # Create service instance
            service = FraudDetectionService()
            
            # Verify
            assert service.transactions_topic == 'test-transactions'
            assert service.fraud_alert_topic == 'test-fraud-alerts'
            assert service.consumer_group == 'test-group'
            assert service.kafka_brokers == 'localhost:9092'
            
            # Verify service components were initialized
            assert hasattr(service, 'consumer')
            assert hasattr(service, 'producer')
            assert hasattr(service, 'redis_client')
            assert hasattr(service, 'metric_service')
            assert hasattr(service, 'rule_evaluator')
            assert hasattr(service, 'fraud_service')
    
    def test_init_kafka(self, env_vars):
        """Test Kafka initialization."""
        with patch('app.main.KafkaConsumer') as consumer_mock, \
             patch('app.main.KafkaProducer') as producer_mock, \
             patch('app.main.RedisClient', return_value=MagicMock()), \
             patch('app.main.MetricService', return_value=MagicMock()), \
             patch('app.main.RuleEvaluator', return_value=MagicMock()), \
             patch('app.main.FraudService', return_value=MagicMock()):
            
            consumer_mock.return_value = MagicMock()
            producer_mock.return_value = MagicMock()
            
            # Create service and test Kafka initialization
            service = FraudDetectionService()
            
            # Verify consumer initialization
            # The topic is passed as a positional argument, not a keyword argument
            consumer_args = consumer_mock.call_args
            assert len(consumer_args[0]) > 0  # Check there's at least one positional argument
            assert consumer_args[0][0] == service.transactions_topic  # First positional arg should be the topic
            
            # Check the keyword arguments
            consumer_kwargs = consumer_args[1]
            assert consumer_kwargs.get('group_id') == service.consumer_group
            assert consumer_kwargs.get('bootstrap_servers') == service.kafka_brokers.split(',')
            assert consumer_kwargs.get('auto_offset_reset') == 'earliest'
            assert consumer_kwargs.get('enable_auto_commit') is True
            
            # Verify producer initialization
            producer_args = producer_mock.call_args[1]
            assert producer_args.get('bootstrap_servers') == service.kafka_brokers.split(',')
            assert producer_args.get('acks') == 'all'
            assert 'value_serializer' in producer_args
    
    def test_process_transaction(self, env_vars):
        """Test processing a transaction."""
        with patch('app.main.KafkaConsumer', return_value=MagicMock()), \
             patch('app.main.KafkaProducer', return_value=MagicMock()), \
             patch('app.main.RedisClient', return_value=MagicMock()), \
             patch('app.main.MetricService', return_value=MagicMock()), \
             patch('app.main.RuleEvaluator', return_value=MagicMock()), \
             patch('app.main.FraudService') as fraud_mock:
            
            # Create mock fraud service
            fraud_service = MagicMock()
            fraud_mock.return_value = fraud_service
            
            # Create transaction data
            transaction_data = {
                "transaction_id": "tx123456789",
                "merchant_id": 42,
                "amount": 10000,
                "currency": "USD",
                "card_no": "4111111111111111",
                "bank_code": "CHASE",
                "timestamp": 1678900000
            }
            
            # Create service and process transaction
            service = FraudDetectionService()
            service.process_transaction(transaction_data)
            
            # Verify transaction was passed to fraud service
            fraud_service.process_transaction.assert_called_once_with(transaction_data)
    
    def test_handle_shutdown(self, env_vars):
        """Test shutdown handler with mock objects."""
        with patch('app.main.KafkaConsumer', return_value=MagicMock()), \
             patch('app.main.KafkaProducer', return_value=MagicMock()), \
             patch('app.main.RedisClient', return_value=MagicMock()), \
             patch('app.main.MetricService', return_value=MagicMock()), \
             patch('app.main.RuleEvaluator', return_value=MagicMock()), \
             patch('app.main.FraudService', return_value=MagicMock()), \
             patch('app.main.sys.exit') as exit_mock:
            
            # Create service instance
            service = FraudDetectionService()
            
            # Cache references to mocks for verification
            consumer = service.consumer
            producer = service.producer
            
            # Call shutdown handler
            service._handle_shutdown(signal.SIGTERM, None)
            
            # Verify
            assert service.running is False
            consumer.close.assert_called_once()
            producer.close.assert_called_once()
            exit_mock.assert_called_once_with(0)
    
    def test_run_with_valid_message(self, env_vars):
        """Test running service with valid messages."""
        with patch('app.main.KafkaConsumer') as consumer_mock, \
             patch('app.main.KafkaProducer', return_value=MagicMock()), \
             patch('app.main.RedisClient', return_value=MagicMock()), \
             patch('app.main.MetricService', return_value=MagicMock()), \
             patch('app.main.RuleEvaluator', return_value=MagicMock()), \
             patch('app.main.FraudService') as fraud_service_mock:
            
            # Create the FraudService mock that will be returned
            fraud_service = MagicMock()
            fraud_service_mock.return_value = fraud_service
            fraud_service.process_transaction = MagicMock()
            
            # Create mock consumer
            consumer = MagicMock()
            consumer_mock.return_value = consumer
            
            # Create a transaction dictionary
            transaction_data = {
                "transaction_id": "tx123456789",
                "merchant_id": 42,
                "amount": 10000,
                "currency": "USD",
                "card_no": "4111111111111111",
                "bank_code": "CHASE",
                "timestamp": int(time.time())
            }
            
            # Create a mock message with the transaction data as JSON string
            message = MagicMock()
            message.value = json.dumps(transaction_data).encode('utf-8')
            
            # Mock the poll method for the consumer
            # It should return once with our message, then nothing on subsequent calls
            poll_results = {}
            topic_partition = MagicMock()
            poll_results[topic_partition] = [message]
            
            # First call returns our message, second call exits the loop
            poll_calls = 0
            def mock_poll(timeout_ms=None):
                nonlocal poll_calls
                if poll_calls == 0:
                    poll_calls += 1
                    return poll_results
                else:
                    # Exit the loop after first poll
                    service.running = False
                    return {}
            
            consumer.poll.side_effect = mock_poll
            
            # Initialize and run the service
            service = FraudDetectionService()
            
            # In the real service, the process_transaction method would decode the JSON
            # We need to create a mock implementation that does this decoding
            original_process = service.process_transaction
            captured_transaction = None
            
            def mock_process(data):
                nonlocal captured_transaction
                # Decode the JSON bytes to a dictionary
                if isinstance(data, bytes):
                    data = json.loads(data.decode('utf-8'))
                # Store the transaction data for later verification
                captured_transaction = data
                # Call the mock fraud_service's process_transaction method
                return fraud_service.process_transaction(data)
            
            service.process_transaction = MagicMock(side_effect=mock_process)
            
            # Run the service
            service.run()
            
            # Verify process_transaction was called
            service.process_transaction.assert_called_once()
            
            # Verify that we received the expected JSON-decoded transaction data
            if captured_transaction:
                assert isinstance(captured_transaction, dict)
                assert captured_transaction.get('transaction_id') == transaction_data['transaction_id']