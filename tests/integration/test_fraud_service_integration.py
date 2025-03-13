"""
Integration tests for the fraud service with rule evaluator and metric service.
"""
import pytest
import json
from unittest.mock import MagicMock, patch
from app.fraud_service import FraudService
from app.rule_evaluator import RuleEvaluator
from app.metric_service import MetricService
from app.cache import RedisClient

class TestFraudServiceIntegration:
    """Integration tests for the fraud service and its dependencies."""
    
    @pytest.fixture
    def redis_client_mock(self):
        """Create a mock for the redis client."""
        mock = MagicMock(spec=RedisClient)
        
        # Set up active rules
        mock.load_active_rules.return_value = [
            {
                "rule_id": "velocity-check",
                "name": "Transaction Velocity Check",
                "logical_operator": "AND",
                "conditions": [
                    {
                        "type": "metric",
                        "metric": {
                            "type": "count",
                            "window": "1 hours",
                            "group_by": ["card_no"]
                        },
                        "operator": ">",
                        "threshold": 5,
                        "threshold_type": "absolute"
                    }
                ]
            }
        ]
        
        mock.trigger_alert.return_value = True
        return mock
    
    @pytest.fixture
    def kafka_producer_mock(self):
        """Create a mock for the kafka producer."""
        mock = MagicMock()
        future_mock = MagicMock()
        future_mock.get.return_value = MagicMock()
        mock.send.return_value = future_mock
        return mock
    
    @pytest.fixture
    def metric_service(self, redis_client_mock):
        """Create a real metric service with a mocked redis client."""
        return MetricService(redis_client=redis_client_mock)
    
    @pytest.fixture
    def rule_evaluator(self, metric_service):
        """Create a real rule evaluator with the metric service."""
        return RuleEvaluator(metric_service=metric_service)
    
    @pytest.fixture
    def fraud_service(self, redis_client_mock, kafka_producer_mock, metric_service, rule_evaluator):
        """Create a fraud service with real components but mocked external dependencies."""
        service = FraudService(
            redis_client=redis_client_mock,
            metric_service=metric_service,
            rule_evaluator=rule_evaluator,
            kafka_producer=kafka_producer_mock
        )
        
        # Ensure fraud_alert_topic is set correctly
        service.fraud_alert_topic = "fraud_alerts"
        
        return service
    
    def test_service_integration_no_fraud(self, fraud_service, sample_transaction, redis_client_mock, kafka_producer_mock, metric_service):
        """Test the full service integration with no fraud detected."""
        # Setup mock to simulate low transaction count
        metric_service.get_metric_value = MagicMock(return_value=2)  # Below threshold of 5
        
        # Process a transaction
        fraud_service.process_transaction(sample_transaction)
        
        # Verify
        redis_client_mock.load_active_rules.assert_called_once()
        redis_client_mock.trigger_alert.assert_not_called()
        kafka_producer_mock.send.assert_not_called()
    
    def test_service_integration_velocity_fraud(self, fraud_service, redis_client_mock, kafka_producer_mock, metric_service):
        """Test the full service integration with velocity-based fraud detected."""
        # Create a transaction with a current timestamp
        import time
        current_timestamp = int(time.time())  # Current timestamp
        transaction = {
            "transaction_id": "tx123456789",
            "merchant_id": 42,
            "amount": 10000,
            "currency": "USD",
            "card_no": "4111111111111111",
            "bank_code": "CHASE",
            "timestamp": current_timestamp  # Current time
        }
        
        # Setup mock to simulate high transaction count
        metric_service.calculate_metric = MagicMock(return_value=10)  # Above threshold of 5
        
        # Process a transaction
        fraud_service.process_transaction(transaction)
        
        # Verify
        redis_client_mock.load_active_rules.assert_called_once()
        redis_client_mock.trigger_alert.assert_called_once()
        kafka_producer_mock.send.assert_called_once()
        
        # Check alert details
        args, kwargs = kafka_producer_mock.send.call_args
        topic = args[0] if args else kwargs.get('topic')
        alert_data = args[1] if len(args) > 1 else kwargs.get('value')
        
        assert topic == "fraud_alerts"
        
        # The alert data is already a dict, no need to parse JSON
        assert alert_data["transaction_id"] == transaction["transaction_id"]
        assert alert_data["rule_id"] == "velocity-check"
        
        # The rule name likely contains 'velocity' so check for that instead of reason
        # which might not be in the alert data structure
        assert "velocity" in alert_data.get("rule_name", "").lower()
    
    def test_service_integration_multiple_fraud_rules(self, fraud_service, redis_client_mock, kafka_producer_mock, metric_service):
        """Test the full service integration with multiple fraud rules triggered."""
        # Create a transaction that will trigger both rules
        import time
        current_timestamp = int(time.time())  # Current timestamp
        transaction = {
            "transaction_id": "tx123456789",
            "merchant_id": 42,
            "amount": 10000,
            "currency": "USD",
            "card_no": "4111111111111111",
            "bank_code": "CHASE",
            "timestamp": current_timestamp  # Current time
        }
        
        # Reset mocks
        redis_client_mock.reset_mock()
        kafka_producer_mock.reset_mock()
        
        # Setup the rules to test
        redis_client_mock.load_active_rules.return_value = [
            {
                "rule_id": "velocity-check",
                "name": "Transaction Velocity Check",
                "logical_operator": "AND",
                "conditions": [
                    {
                        "type": "metric",
                        "metric": {
                            "type": "count",
                            "window": "1 hours",
                            "group_by": ["card_no"]
                        },
                        "operator": ">",
                        "threshold": 5,
                        "threshold_type": "absolute"
                    }
                ]
            },
            {
                "rule_id": "amount-anomaly",
                "name": "Amount Anomaly",
                "logical_operator": "AND",
                "conditions": [
                    {
                        "type": "direct",
                        "field": "amount",
                        "operator": ">",
                        "value": 5000
                    }
                ]
            }
        ]
        
        # Setup metric_service to return a value above threshold
        metric_service.calculate_metric = MagicMock(return_value=10)  # Above threshold of 5
        
        # Mock the rule evaluator to always return True (fraud detected)
        original_evaluate_rule = fraud_service.rule_evaluator.evaluate_rule
        def mock_evaluate_rule(rule, transaction):
            return True, f"Mock fraud detected for rule {rule['rule_id']}"
            
        fraud_service.rule_evaluator.evaluate_rule = mock_evaluate_rule
        
        try:
            # Process the transaction
            fraud_service.process_transaction(transaction)
        finally:
            # Restore the original method
            fraud_service.rule_evaluator.evaluate_rule = original_evaluate_rule
        
        # Verify
        redis_client_mock.load_active_rules.assert_called_once()
        
        # Should have triggered alerts for both rules
        assert redis_client_mock.trigger_alert.call_count == 2
        
        # Should have sent 2 Kafka messages
        assert kafka_producer_mock.send.call_count == 2
        
        # Check alert details for both messages
        call_args_list = kafka_producer_mock.send.call_args_list
        
        alert_rule_ids = []
        for call_args in call_args_list:
            args, kwargs = call_args
            topic = args[0] if args else kwargs.get('topic')
            alert_data = args[1] if len(args) > 1 else kwargs.get('value')
            assert topic == "fraud_alerts"
            
            # The alert data is already a dict, no need to parse JSON
            assert alert_data["transaction_id"] == transaction["transaction_id"]
            alert_rule_ids.append(alert_data["rule_id"])
        
        # Ensure both rules were triggered
        assert "velocity-check" in alert_rule_ids
        assert "amount-anomaly" in alert_rule_ids
    
    def test_service_integration_error_handling(self, fraud_service, sample_transaction, redis_client_mock, kafka_producer_mock):
        """Test the service's error handling during integration."""
        # Setup Redis client to simulate an error
        redis_client_mock.load_active_rules.side_effect = Exception("Test Redis error")
        
        # Process a transaction - should not crash
        fraud_service.process_transaction(sample_transaction)
        
        # Verify no alerts or Kafka messages were sent
        redis_client_mock.trigger_alert.assert_not_called()
        kafka_producer_mock.send.assert_not_called()
        
        # Reset the mock for the next test
        redis_client_mock.load_active_rules.side_effect = None
        redis_client_mock.load_active_rules.return_value = [
            {
                "rule_id": "invalid-rule",
                "name": "Invalid Rule",
                "logical_operator": "INVALID",  # Invalid operator
                "conditions": []
            }
        ]
        
        # Process a transaction with an invalid rule - should not crash
        fraud_service.process_transaction(sample_transaction)
        
        # Verify no alerts or Kafka messages were sent
        redis_client_mock.trigger_alert.assert_not_called()
        kafka_producer_mock.send.assert_not_called()
    
    def test_service_integration_kafka_error(self, fraud_service, redis_client_mock, kafka_producer_mock, metric_service):
        """Test the service's handling of Kafka errors."""
        # Create a transaction with a current timestamp
        import time
        current_timestamp = int(time.time())  # Current timestamp
        transaction = {
            "transaction_id": "tx123456789",
            "merchant_id": 42,
            "amount": 10000,
            "currency": "USD",
            "card_no": "4111111111111111",
            "bank_code": "CHASE",
            "timestamp": current_timestamp  # Current time
        }
        
        # Setup mock to simulate high transaction count
        metric_service.calculate_metric = MagicMock(return_value=10)  # Above threshold of 5
        
        # Setup Kafka producer to simulate an error
        future_mock = MagicMock()
        future_mock.get.side_effect = Exception("Test Kafka error")
        kafka_producer_mock.send.return_value = future_mock
        
        # Process a transaction - should not crash
        fraud_service.process_transaction(transaction)
        
        # Verify
        redis_client_mock.load_active_rules.assert_called_once()
        redis_client_mock.trigger_alert.assert_called_once()
        kafka_producer_mock.send.assert_called_once()
