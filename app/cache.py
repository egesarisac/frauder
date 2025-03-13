import os
import redis
import structlog
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from redis import Redis, ConnectionError, TimeoutError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = structlog.get_logger()

class RedisClient:
    """Redis client for caching and rule management.
    
    This class handles all interactions with Redis, including:
    - Loading fraud detection rules
    - Storing transaction metrics
    - Triggering and logging fraud alerts
    """
    
    def __init__(self) -> None:
        """Initialize Redis connection using environment variables."""
        try:
            self.redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'redis'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                db=0,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Redis client initialized successfully")
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Redis connection error: {str(e)}")
            # Still create the client, but operations will fail later if Redis isn't available
            self.redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'redis'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                db=0,
                decode_responses=True
            )

    def load_active_rules(self) -> List[Dict[str, Any]]:
        """Load active fraud detection rules from Redis.
        
        Returns:
            List of rule dictionaries, each containing rule configuration data.
            Returns empty list if no rules are found or on Redis errors.
        """
        try:
            rule_ids = self.redis_client.smembers("active_rule_ids")
            rules = []
            
            for rule_id in rule_ids:
                try:
                    rule_data = self.redis_client.hget(f"fraud_rules:{rule_id}", "data")
                    if rule_data:
                        rule_dict = json.loads(rule_data)
                        # Ensure rule_id is included in the rule dictionary
                        if "rule_id" not in rule_dict:
                            rule_dict["rule_id"] = rule_id
                        rules.append(rule_dict)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid rule data format for rule {rule_id}: {str(e)}")
                    continue
                    
            logger.info(f"Loaded {len(rules)} active rules from Redis")
            return rules
        except Exception as e:
            logger.error(f"Error loading rules from Redis: {str(e)}")
            return []

    def trigger_alert(self, transaction: Dict[str, Any], rule: Dict[str, Any]) -> None:
        """Record a fraud alert for a matched rule.
        
        Args:
            transaction: Transaction data dictionary
            rule: Rule configuration that was triggered
        """
        try:
            # Store alert in Redis (useful for tracking and dashboards)
            alert_key = f"fraud_alerts:{transaction.get('transaction_id', 'unknown')}"
            alert_data = {
                "rule_id": rule.get("rule_id"),
                "rule_name": rule.get("name"),
                "merchant_id": transaction.get("merchant_id"),
                "amount": transaction.get("amount"),
                "timestamp": transaction.get("timestamp", int(datetime.now().timestamp())),
                "detection_time": int(datetime.now().timestamp())
            }
            
            # Store alert data
            self.redis_client.hmset(alert_key, alert_data)
            # Set expiration (7 days)
            self.redis_client.expire(alert_key, 604800)
            # Add to alerts set for this rule
            self.redis_client.sadd(f"rule_alerts:{rule.get('rule_id')}", transaction.get("transaction_id", "unknown"))
            
            logger.info(
                "Fraud alert triggered",
                rule_id=rule.get("rule_id"),
                rule_name=rule.get("name"),
                transaction_id=transaction.get("transaction_id"),
                merchant_id=transaction.get("merchant_id"),
                amount=transaction.get("amount")
            )
        except Exception as e:
            logger.error(f"Error storing fraud alert in Redis: {str(e)}")
    
    # Forward Redis methods to self.redis_client for convenience
    def pipeline(self):
        """Get a Redis pipeline for batch operations."""
        return self.redis_client.pipeline()
        
    def ping(self) -> bool:
        """Ping the Redis server to test connectivity."""
        try:
            self.redis_client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis ping failed: {str(e)}")
            return False
    
    def keys(self, pattern: str) -> List[str]:
        """Get Redis keys matching the pattern."""
        try:
            return self.redis_client.keys(pattern)
        except Exception as e:
            logger.error(f"Error getting Redis keys: {str(e)}")
            return []
    
    def exists(self, key: str) -> bool:
        """Check if a key exists in Redis."""
        try:
            return bool(self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Error checking if key exists: {str(e)}")
            return False
    
    def zrangebyscore(self, key: str, min: Union[int, str], max: Union[int, str], withscores: bool = False) -> List[Any]:
        """Get sorted set values within a score range."""
        try:
            return self.redis_client.zrangebyscore(key, min, max, withscores=withscores)
        except Exception as e:
            logger.error(f"Error getting sorted set range: {str(e)}")
            return []
            
    def get_sorted_set_values(self, key: str, start: int = 0, end: int = -1) -> List[str]:
        """Get values from a sorted set by their index range.
        
        Args:
            key: The key of the sorted set
            start: The starting index (inclusive)
            end: The ending index (inclusive)
            
        Returns:
            List of sorted set values as strings
        """
        try:
            values = self.redis_client.zrevrange(key, start, end)
            # Convert bytes to strings if needed
            return [v.decode() if isinstance(v, bytes) else v for v in values]
        except Exception as e:
            logger.error(f"Error getting sorted set values: {str(e)}")
            return []