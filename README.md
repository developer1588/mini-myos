# Setup

## Tests and setup

A docker is required to run the tests and setup the localstack environment. The tests are written in TypeScript and use Jest as a test runner. The project uses AWS CDK to deploy the localstack environment.

For issues with the docker setup please see `compose.yml` and adjust the volumes.

Run the following commands to set up the project:

```bash
# Install dependencies
npx npm install

# Build the localstack environment
# Preferably run this in a separate terminal
npx npm run test:setup

# Deploy localstack locally
npx npm run test:deploy

# Run the tests
npx npm run test:run

# Run linter
npx npm run lint
```

## Agent tests

Currently there is one simple agent implementation that can be used to test the system. It is a part of cdk stack and can be found in `lambda/agent.ts`. It is not needed to run the code, but it can be used to test the system.

Agent tests are part of the test suite. But can be run separately with the following command (assuming the localstack is running):

```bash
npx jest --maxWorkers=1 tests/integration/agent.test.ts
```

## Decision reasonings

The architecture was built to be the most cost-effective as possible, however, some decisions were made to ensure the system is scalable and maintainable (i.e. using serverless services like Lambda, DynamoDB, and Kinesis). The biggest problem was to maintain the order of the events for each subscriber with the given constraints, as the typical SNS + SQS setup has lower limits on throughput.

In order to ensure the events processing guarantees, the settings like Maximum batch size of the DynamoDB stream can be adjusted (i.e. to add some delay before the events are processed).

## Future improvements

- Decouple CDK stack into smaller modules with Constructs.
- Tweak the settings of the Kinesis Data Stream, DynamoDB Streams, and SQS FIFO queue to optimize the performance and cost.
- Perform a test load to see how the system behaves under high traffic.
- Consider changing Lambda into Step Functions to avoid concurrent Lambda limitations.

# Architecture Overview

## High level diagram

                 +-------------+
                 |  Producers  |
                 +-------------+
                        |
            -------------------------------
            |                             |
            \/                            \/
    +--------------+              +-----------------+
    | POST /events |              | Direct PutRecord|
    | (API Gateway)|------------->| to Kinesis DS   |
    +--------------+  (optional)  +-----------------+
                                            |
                                            \/
                                  +----------------+
                                  | Kinesis Data   |
                                  | Stream         |
                                  +----------------+
                                            |
                                            | (per shard trigger)
                                            |
                                            \/
                                   +----------------------+
                                   | Lambda "Aggregator"  |
                                   | - Consumes Stream    |
                                   | - Reads Subscribers  |
                                   | - Writes to DynamoDB |
                                   +----------------------+
                                            |
                                            \/
                                   +------------------------+
                                   | DynamoDB               |
                                   | SubscriberEvents Table |
                                   +------------------------+
                                            |
                                            | (DynamoDB Streams)
                                            |
                                            \/
                                   +--------------------------+
                                   | lambda "Publisher"       |
                                   | - Reads DynamoDB stream  |
                                   | - Sends events to SQS    |
                                   +--------------------------+
                                            |
                                            \/
                                   +----------------+
                                   | SQS Queue FIFO |
                                   +----------------+
                                            |
                                            \/
                                   +---------------+
                                   |   Agents      |
                                   | (consume msgs)|
                                   +---------------+


## Explanation

### Flow

#### Kinesis Data Stream (KDS) as Buffer
:
Producers publish events to a Kinesis Data Stream, either via API Gateway (POST /events) or direct PutRecord calls to Kinesis (to reduce API Gateway costs at scale). KDS provides a durable, partitioned buffer (24-hour retention) for parallel downstream processing, partitioned by `agentId`.

Example:

    await kinesis.putRecord({
      StreamName:   EVENTS_STREAM,
      PartitionKey: event.agentId,
      Data:         JSON.stringify(event),
    });

Each event may have the form:

    {
      "agentId": "unique-agent-id",
      "timestamp": "2025-07-01T12:00:00.000Z",
      "eventType": "exampleEvent",
      "data": { /* payload */ }
    }

#### Aggregator Lambda Function

A Lambda function, triggered per Kinesis shard, groups incoming records by agentId, retrieves the subscribers for each agentId from the Subscribers table, and writes one event entry per subscriber into the SubscriberEvents DynamoDB table. The table’s composite primary key (subscriberId, timestamp) ensures efficient, timestamp-ordered queries.

#### Publisher Lambda Function

Triggered by DynamoDB Streams on the SubscriberEvents table, this Lambda reads new items, groups them by subscriberId, sorts by timestamp, and sends ordered batches of messages to a dedicated SQS FIFO queue per subscriber. FIFO queues preserve message order and deduplicate via MessageDeduplicationId.

#### Agent Consumption

Each agent polls its own SQS FIFO queue for messages. FIFO semantics guarantee that messages are delivered in the exact order they were written, allowing agents to process events sequentially. Agents can use long polling to reduce costs and improve efficiency.

# API Reference
## 1. Register Agent

**Endpoint**

    POST /agents

**Description**

Register a new agent with a resource ARN. If an agent with the same resourceArn already exists, returns the existing agent data. Otherwise, creates a new agent with a unique agentId and dedicated SQS FIFO queue, then grants the resourceArn permission to poll the queue.

**Request**

    {
        "resourceArn": "arn:aws:iam::123456789012:role/MyRole"
    }

**Response**

200 OK (existing agent) or 200 OK (new agent)

    {
        "agentId": "unique-agent-id",
        "queueUrl": "https://sqs.region.amazonaws.com/account/agents-agentId.fifo",
        "resourceArn": "arn:aws:iam::123456789012:role/MyRole"
    }

400 Bad Request - resourceArn is required
500 Internal Server Error - server error

## 2. Subscribe to a Producer

**Endpoint**

    PUT /agents/{listenerId}/subscriptions/{producerId}

**Description**

Writes a record in the Subscribers table linking the agentId (listenerId) to the producerId. Idempotent.

**Request**

    {}

**Response**

204 No Content – subscription created (or already existed)
404 Not Found – either listenerId or producerId doesn’t exist (optional validation)

# DynamoDB Tables

## Agents Table
- **Table Name**: Agents
- **Primary Key**:
  - Partition Key: `agentId` (string)
- **Global Secondary Index**: `resourceArn-index`
- **Attributes**:
  - `agentId` (string): Unique identifier for the agent.
  - `queueUrl` (string): The SQS FIFO queue URL for this agent.
  - `createdAt` (string, ISO 8601 format): The timestamp when the agent was created.

## SubscriberEvents Table

- **Table Name**: SubscriberEvents
- **Primary Key**:
  - Partition Key: `subscriberId` (string)
  - Sort Key: `timestamp` (string, ISO 8601 format with milliseconds)
- **Attributes**:
    - `data` (map): The event data.

## Subscribers Table

- **Table Name**: Subscribers
- **Primary Key**:
  - Partition Key: `producerId` (string)
  - Sort Key: `subscriberId` (string)
- **Attributes**:
    - `createdAt` (string, ISO 8601 format): The timestamp when the record was created.
