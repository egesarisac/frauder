"""
Unit tests for the Fraud Service component.
"""
import pytest
from unittest.mock import MagicMock, patch, call
import json
from app.fraud_service import FraudService
from app.models import Transaction

class TestFraudService:
    """Tests for the FraudService class."""
    
    @pytest.fixture
    def metric_service_mock(self):
        """Create a mock for the MetricService."""
        mock = MagicMock()
        mock.update_metrics.return_value = None
        mock.get_metrics_for_rule.return_value = {"count": 10}
        return mock
    
    @pytest.fixture
    def rule_evaluator_mock(self):
        """Create a mock for the RuleEvaluator."""
        mock = MagicMock()
        mock.evaluate_rule.return_value = (False, "No fraud detected")
        return mock
    
    @pytest.fixture
    def service(self, redis_mock, kafka_producer_mock, metric_service_mock, rule_evaluator_mock):
        """Create a FraudService instance with mocked dependencies."""
        with patch('app.cache.RedisClient', return_value=MagicMock()) as redis_client_mock:
            redis_client_mock.return_value.load_active_rules.return_value = []
            redis_client_mock.return_value.trigger_alert.return_value = True
            
            service = FraudService(
                redis_client=redis_client_mock.return_value,
                kafka_producer=kafka_producer_mock,
                metric_service=metric_service_mock,
                rule_evaluator=rule_evaluator_mock
            )
            
            return service
    
    def test_process_transaction_no_fraud(self, service, sample_transaction, sample_rules):
        """Test processing a transaction with no fraud detected."""
        # Setup
        service.redis_client.load_active_rules.return_value = sample_rules
        
        # Important: We need to reset any previous calls to trigger_alert
        service.redis_client.trigger_alert.reset_mock()
        service.kafka_producer.send.reset_mock()
        
        # Mock evaluate_rule to always return False (no fraud)
        service.rule_evaluator.evaluate_rule.return_value = False
        
        # Execute
        service.process_transaction(sample_transaction)
        
        # Verify
        service.redis_client.load_active_rules.assert_called_once()
        service.metric_service.update_metrics.assert_called_once_with(sample_rules, sample_transaction)
        
        # Should have evaluated all rules in the sample (which is 2)
        assert service.rule_evaluator.evaluate_rule.call_count == 2
        
        # Should not have triggered any alerts or sent kafka messages
        service.redis_client.trigger_alert.assert_not_called()
        service.kafka_producer.send.assert_not_called()
    
    def test_process_transaction_fraud_detected(self, service, sample_transaction, sample_rules):
        """Test processing a transaction with fraud detected."""
        # Setup
        service.redis_client.load_active_rules.return_value = sample_rules
        
        # Reset mocks to clear any previous calls
        service.redis_client.trigger_alert.reset_mock()
        service.kafka_producer.send.reset_mock()
        
        # Create a side effect function that returns True for the amount-anomaly-1 rule
        def evaluate_rule_side_effect(rule, transaction):
            if rule["rule_id"] == "amount-anomaly-1":
                return True
            return False
        
        service.rule_evaluator.evaluate_rule.side_effect = evaluate_rule_side_effect
        
        # Execute
        service.process_transaction(sample_transaction)
        
        # Verify
        service.redis_client.load_active_rules.assert_called_once()
        service.metric_service.update_metrics.assert_called_once_with(sample_rules, sample_transaction)
        
        # Should have evaluated all rules in the sample (which is 2)
        assert service.rule_evaluator.evaluate_rule.call_count == 2
        
        # Should have triggered an alert for the amount-anomaly-1 rule
        service.redis_client.trigger_alert.assert_called_once()
        
        # Should have sent a kafka message
        service.kafka_producer.send.assert_called_once()
        
        # Verify the kafka message contents
        call_args = service.kafka_producer.send.call_args
        # The first argument is the topic, the second is a keyword argument 'value'
        topic = call_args[0][0]
        alert_data = call_args[1]['value']
        
        assert topic == "fraud_alerts"
        assert alert_data["transaction_id"] == sample_transaction["transaction_id"]
        assert alert_data["rule_id"] == "amount-anomaly-1"
    
    def test_process_transaction_invalid_data(self, service):
        """Test processing a transaction with invalid data."""
        # Setup
        invalid_transaction = {
            "transaction_id": "tx123456789",
            "amount": -100,  # Invalid amount
            "currency": "USD",
            "card_no": "4111111111111111",
            "bank_code": "CHASE",
            "timestamp": 1678900000,
            # Missing merchant_id field
        }
        
        # Execute
        service.process_transaction(invalid_transaction)
        
        # Verify
        # Should not have loaded rules or processed anything
        service.redis_client.load_active_rules.assert_not_called()
        service.metric_service.update_metrics.assert_not_called()
        service.rule_evaluator.evaluate_rule.assert_not_called()
        service.redis_client.trigger_alert.assert_not_called()
        service.kafka_producer.send.assert_not_called()
    
    def test_process_transaction_rule_error(self, service, sample_transaction, sample_rules):
        """Test processing a transaction where a rule evaluation throws an error."""
        # Setup
        service.redis_client.load_active_rules.return_value = sample_rules
        
        # Reset mocks to clear any previous calls
        service.redis_client.trigger_alert.reset_mock()
        service.kafka_producer.send.reset_mock()
        
        # The second rule throws an exception
        def evaluate_rule_side_effect(rule, transaction):
            if rule["rule_id"] == "amount-anomaly-1":
                raise Exception("Test error in rule evaluation")
            return False
        
        service.rule_evaluator.evaluate_rule.side_effect = evaluate_rule_side_effect
        
        # Execute
        service.process_transaction(sample_transaction)
        
        # Verify
        service.redis_client.load_active_rules.assert_called_once()
        service.metric_service.update_metrics.assert_called_once_with(sample_rules, sample_transaction)
        
        # Should have evaluated all rules in the sample (which is 2) despite the error
        assert service.rule_evaluator.evaluate_rule.call_count == 2
        
        # Should not have triggered any alerts or sent Kafka messages
        service.redis_client.trigger_alert.assert_not_called()
        service.kafka_producer.send.assert_not_called()

    def test_process_transaction_with_multiple_fraud_rules(self, service, sample_transaction, sample_rules):
        """Test processing a transaction that triggers multiple fraud rules."""
        # Setup
        service.redis_client.load_active_rules.return_value = sample_rules
        
        # Reset mocks to clear any previous calls
        service.redis_client.trigger_alert.reset_mock()
        service.kafka_producer.send.reset_mock()
        
        # Both rules detect fraud
        def evaluate_rule_side_effect(rule, transaction):
            return True  # All rules detect fraud
        
        service.rule_evaluator.evaluate_rule.side_effect = evaluate_rule_side_effect
        
        # Execute
        service.process_transaction(sample_transaction)
        
        # Verify
        service.redis_client.load_active_rules.assert_called_once()
        service.metric_service.update_metrics.assert_called_once_with(sample_rules, sample_transaction)
        
        # Should have evaluated all rules in the sample (which is 2)
        assert service.rule_evaluator.evaluate_rule.call_count == 2
        
        # Should have triggered alerts for both rules
        assert service.redis_client.trigger_alert.call_count == 2
        
        # Should have sent 2 Kafka messages
        assert service.kafka_producer.send.call_count == 2
        
        # Verify the Kafka message contents for both rules
        call_args_list = service.kafka_producer.send.call_args_list
        
        # Get rule IDs from the sample rules
        rule_ids = [rule["rule_id"] for rule in sample_rules]
        
        # First alert
        topic1 = call_args_list[0][0][0]  # First call, first positional arg (topic)
        alert_data1 = call_args_list[0][1]['value']  # First call, keyword arg 'value'
        assert topic1 == "fraud_alerts"
        assert alert_data1["rule_id"] in rule_ids
        
        # Second alert
        topic2 = call_args_list[1][0][0]  # Second call, first positional arg (topic)
        alert_data2 = call_args_list[1][1]['value']  # Second call, keyword arg 'value'
        assert topic2 == "fraud_alerts"
        assert alert_data2["rule_id"] in rule_ids
        
        # Ensure they're different rules
        assert alert_data1["rule_id"] != alert_data2["rule_id"]
