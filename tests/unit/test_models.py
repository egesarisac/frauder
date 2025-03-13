"""
Unit tests for the Transaction model.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from app.models import Transaction

class TestTransactionModel:
    """Tests for the Transaction data model."""

    def test_valid_transaction(self, sample_transaction):
        """Test that a valid transaction passes validation."""
        # Should not raise an exception
        transaction = Transaction(**sample_transaction)
        assert transaction.transaction_id == sample_transaction["transaction_id"]
        assert transaction.merchant_id == sample_transaction["merchant_id"]
        assert transaction.amount == sample_transaction["amount"]
        assert transaction.currency == sample_transaction["currency"]
        # Card number should be masked
        assert transaction.card_no != sample_transaction["card_no"]
        assert transaction.card_no.startswith("411111")
        assert transaction.card_no.endswith("1111")
        assert "*" in transaction.card_no
        assert transaction.bank_code == sample_transaction["bank_code"]
        assert transaction.timestamp == sample_transaction["timestamp"]

    def test_invalid_amount(self, sample_transaction):
        """Test that an invalid amount fails validation."""
        invalid_data = sample_transaction.copy()
        invalid_data["amount"] = -100
        
        with pytest.raises(ValidationError) as excinfo:
            Transaction(**invalid_data)
        
        # Check that the error is related to the amount field
        assert "amount" in str(excinfo.value)
        assert "greater than" in str(excinfo.value)

    def test_invalid_currency(self, sample_transaction):
        """Test that an invalid currency code fails validation."""
        invalid_data = sample_transaction.copy()
        invalid_data["currency"] = "INVALID"
        
        with pytest.raises(ValidationError) as excinfo:
            Transaction(**invalid_data)
        
        # Check that the error is related to the currency field
        assert "currency" in str(excinfo.value)
        # The error message will contain 'String should have at most 3 characters' for too long currency
        assert "characters" in str(excinfo.value)
        
        # Test lowercase currency
        invalid_data["currency"] = "usd"
        with pytest.raises(ValidationError) as excinfo:
            Transaction(**invalid_data)
            
        # Check that the error message contains the custom validator message
        assert "currency" in str(excinfo.value)
        assert "ISO 4217" in str(excinfo.value)

    def test_invalid_timestamp(self, sample_transaction):
        """Test that an invalid timestamp fails validation."""
        invalid_data = sample_transaction.copy()
        # Future timestamp (1 year from now)
        future_time = int(datetime.now().timestamp()) + 31536000
        invalid_data["timestamp"] = future_time
        
        with pytest.raises(ValidationError) as excinfo:
            Transaction(**invalid_data)
        
        # Check that the error is related to the timestamp field
        assert "timestamp" in str(excinfo.value)
        assert "future" in str(excinfo.value)

    def test_card_number_masking(self, sample_transaction):
        """Test that card numbers are properly masked."""
        transaction = Transaction(**sample_transaction)
        
        # First 6 and last 4 digits should be preserved
        assert transaction.card_no[:6] == sample_transaction["card_no"][:6]
        assert transaction.card_no[-4:] == sample_transaction["card_no"][-4:]
        
        # Middle should be masked
        middle = transaction.card_no[6:-4]
        assert all(char == '*' for char in middle)
        
        # Test with a different card number
        test_data = sample_transaction.copy()
        test_data["card_no"] = "5555555555554444"
        transaction = Transaction(**test_data)
        assert transaction.card_no[:6] == "555555"
        assert transaction.card_no[-4:] == "4444"
        middle = transaction.card_no[6:-4]
        assert all(char == '*' for char in middle)
    
    def test_optional_fields(self, sample_transaction):
        """Test that optional fields can be omitted."""
        minimal_data = {
            "transaction_id": "tx123456789",
            "merchant_id": 42,
            "amount": 10000,
            "currency": "USD",
            "card_no": "4111111111111111",
            "bank_code": "CHASE",
            "timestamp": int(datetime.now().timestamp()) - 3600  # 1 hour ago
        }
