# Fraud Detection Service

This is a Python service for real-time fraud detection that processes transactions from Kafka and applies configurable rules to detect fraudulent activities. When a suspicious transaction is identified, the service sends alerts to a configurable Kafka topic.

## Architecture

The service is built with a modular architecture:

- **Main Service**: Orchestrates Kafka consumer/producer and connects components
- **Fraud Service**: Core business logic for transaction processing and rule application
- **Rule Evaluator**: Flexible rule engine with configurable conditions
- **Metric Service**: Calculates and tracks transaction metrics over time windows
- **Redis Client**: Handles caching, rule storage, and alert tracking

## Features

- **Real-time Transaction Processing**: Consumes transactions from Kafka for immediate processing
- **Dynamic Rule Engine**: Supports complex rules with multiple conditions and logical operators
- **Configurable Fraud Detection Rules**:
  - Velocity checks (e.g., >5 transactions/minute)
  - Amount anomalies (e.g., >200% of 30-day average)
  - Custom rules can be added via Redis
- **Metrics Calculation**: Tracks metrics like transaction counts, averages, and sums
- **Time-Window Analysis**: Evaluates patterns over configurable time windows
- **Fraud Alerts**: Publishes detailed alerts to a Kafka topic when suspicious activities are detected
- **Graceful Error Handling**: Robust error handling and service recovery

## Technical Stack

- **Python 3.9+**
- **Kafka**: For transaction consumption and alert publishing
- **Redis**: For rule storage, metrics caching, and rate limiting
- **Pydantic**: For data validation and transaction modeling
- **Structlog**: For structured logging
- **Docker**: For containerization and deployment

## Setup

### Prerequisites

- Docker and Docker Compose installed
- A running Kafka instance
- A running Redis instance

### Installation

1. Clone the repository
2. Configure environment variables (see below)
3. Build the container:

```bash
docker build -t frauder .
```

## Configuration

### Environment Variables

Create a `.env` file based on the provided `.env.example` with the following variables:

- `KAFKA_BROKERS`: Kafka broker addresses (default: fraud-kafka:29092)
- `TRANSACTIONS_TOPIC`: Kafka topic for incoming transactions (default: transactions)
- `FRAUD_ALERT_TOPIC`: Kafka topic for fraud alerts (default: fraud_alerts)
- `CONSUMER_GROUP`: Kafka consumer group ID (default: fraud-detection-group)
- `REDIS_HOST`: Redis host address (default: redis)
- `REDIS_PORT`: Redis port (default: 6379)

## Running the Service

### Using Docker Compose

The service can be run using Docker Compose:

```bash
docker-compose up frauder
```

### Running Locally for Development

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Run the service:

```bash
python -m app.main
```

## Rule Configuration

Fraud detection rules are stored in Redis and can be dynamically updated. Each rule consists of:

1. **Rule ID**: Unique identifier
2. **Name**: Descriptive name
3. **Conditions**: Array of condition objects (metric-based or direct comparisons)
4. **Logical Operator**: How conditions are combined (AND/OR)

### Rule Examples

#### Example 1: Transaction Velocity Check

Detects when a card is used more than 5 times in an hour:

```json
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
```

This rule would flag cards that are used more than 5 times within a single hour, which is a common pattern in card testing fraud where criminals test stolen card details with multiple small transactions.

#### Example 2: High-Value Transaction with Multiple Conditions

Detects high-value transactions in a specific currency from a specific bank:

```json
{
  "rule_id": "high-value-chase",
  "name": "High Value Chase Bank Transaction",
  "logical_operator": "AND",
  "conditions": [
    {
      "type": "direct",
      "field": "amount",
      "operator": ">",
      "value": 10000
    },
    {
      "type": "direct",
      "field": "currency",
      "operator": "==",
      "value": "USD"
    },
    {
      "type": "direct",
      "field": "bank_code",
      "operator": "==",
      "value": "CHASE"
    }
  ]
}
```

This rule would flag high-value transactions (over $10,000) from a specific bank in USD, which helps monitor large transfers that might require additional verification or reporting under banking regulations.

#### Example 3: Unusual Spending Pattern (OR Conditions)

Detects unusual spending patterns using OR logic:

```json
{
  "rule_id": "unusual-spending",
  "name": "Unusual Spending Pattern",
  "logical_operator": "OR",
  "conditions": [
    {
      "type": "metric",
      "metric": {
        "type": "sum",
        "field": "amount",
        "window": "24 hours",
        "group_by": ["card_no"]
      },
      "operator": ">",
      "threshold": 50000,
      "threshold_type": "absolute"
    },
    {
      "type": "metric",
      "metric": {
        "type": "average",
        "field": "amount",
        "window": "30 days",
        "group_by": ["card_no"]
      },
      "operator": "-",
      "threshold": 200,
      "threshold_type": "percentage"
    }
  ]
}
```

This rule would flag either excessive daily spending (over $50,000 in 24 hours) or transactions that are more than triple the card's 30-day average amount, which helps detect account takeovers where spending patterns suddenly change dramatically.

## Logging

The service uses structured logging with contextual information for better debugging and monitoring. Key events logged include:

- Service startup and configuration
- Transaction processing
- Rule evaluation results
- Fraud alerts
- Error conditions

## Testing

To run tests:

```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

GNU GPLv3

