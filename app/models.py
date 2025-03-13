from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
import re

class Transaction(BaseModel):
    """Transaction data model with validation.
    
    This model represents a financial transaction with essential fields
    and validation rules to ensure data integrity.
    """
    merchant_id: int = Field(..., description="Unique identifier for the merchant")
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    amount: int = Field(..., gt=0, description="Transaction amount in smallest currency unit (e.g., cents)")
    currency: str = Field(..., min_length=3, max_length=3, description="Three-letter currency code (ISO 4217)")
    card_no: str = Field(..., min_length=16, max_length=19, description="Card number (PAN)")
    bank_code: str = Field(..., description="Bank identifier code")
    timestamp: int = Field(..., description="Unix timestamp of the transaction")
    
    @field_validator('currency')
    @classmethod
    def validate_currency(cls, v):
        if not re.match(r'^[A-Z]{3}$', v):
            raise ValueError('currency must be a valid 3-letter ISO 4217 code')
        return v
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v):
        current_time = int(datetime.now().timestamp())
        # Ensure timestamp is not in the future
        if v > current_time:
            raise ValueError('timestamp cannot be in the future')
        return v
    
    @field_validator('card_no')
    @classmethod
    def mask_card_number(cls, v):
        # Store only masked version for logging/security purposes
        if len(v) >= 16:
            return v[:6] + '*' * (len(v) - 10) + v[-4:]
        return v
