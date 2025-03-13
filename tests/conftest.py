"""
Test configuration for pytest.
"""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
import redis
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer

# Add the app directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Redis mock fixtures
@pytest.fixture
def redis_mock():
    """Mock Redis client for testing."""
    redis_mock = MagicMock(spec=redis.Redis)
    return redis_mock

# Kafka mock fixtures
@pytest.fixture
def kafka_producer_mock():
    """Mock Kafka producer for testing."""
    producer_mock = MagicMock(spec=KafkaProducer)
    # Mock the send method to return a future-like object
    future_mock = MagicMock()
    future_mock.get.return_value = MagicMock()  # Mock record metadata
    producer_mock.send.return_value = future_mock
    return producer_mock

@pytest.fixture
def kafka_consumer_mock():
    """Mock Kafka consumer for testing."""
    consumer_mock = MagicMock(spec=KafkaConsumer)
    return consumer_mock

# Sample data fixtures
@pytest.fixture
def sample_transaction():
    """Sample valid transaction data."""
    import time
    current_timestamp = int(time.time())  # Current timestamp
    
    return {
        "transaction_id": "tx123456789",
        "merchant_id": 42,
        "amount": 10000,  # $100.00
        "currency": "USD",
        "card_no": "4111111111111111",
        "bank_code": "CHASE",
        "timestamp": current_timestamp  # Current time
    }

@pytest.fixture
def sample_rule():
    """Sample fraud detection rule."""
    return {
        "rule_id": "velocity-check-1",
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

@pytest.fixture
def sample_rules():
    """Sample set of fraud detection rules."""
    return [
        {
            "rule_id": "velocity-check-1",
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
            "rule_id": "amount-anomaly-1",
            "name": "Amount Anomaly Check",
            "logical_operator": "AND",
            "conditions": [
                {
                    "type": "metric",
                    "metric": {
                        "type": "average",
                        "field": "amount",
                        "window": "30 days",
                        "group_by": ["card_no"]
                    },
                    "operator": ">",
                    "threshold": 200,
                    "threshold_type": "percent"
                }
            ]
        }
    ]
