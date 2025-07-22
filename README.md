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
                                   +-----------------------+
                                   | DynamoDB              |
                                   | SubsriberEvents Table |
                                   +-----------------------+
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

### Motivation

The requirements specify a high volume of events and subscribers, which exceeds the throughput of simple SNS + SQS patterns. This architecture leverages AWS managed services to achieve cost-effective, scalable processing while ensuring strict ordering guarantees.

### Flow

#### Kinesis Data Stream (KDS) as Buffer

Producers publish events to a Kinesis Data Stream, either via API Gateway (POST /events) or direct PutRecord calls to Kinesis (to reduce API Gateway costs at scale). KDS provides a durable, partitioned buffer (24‑hour retention) for parallel downstream processing, partitioned by `agentId`.

Example:

    await kinesis.putRecord({
      StreamName:   EVENTS_STREAM,
      PartitionKey: event.agentId,
      Data:         JSON.stringify(event),
    });

Each event has the form:

    {
      "agentId": "unique-agent-id",
      "timestamp": "2025-07-01T12:00:00.000Z",
      "eventType": "exampleEvent",
      "data": { /* payload */ }
    }

#### Aggregator Lambda Function

A Lambda function, triggered per Kinesis shard, groups incoming records by agentId, retrieves the subscribers for each agentId from the Subscribers table, and writes one event entry per subscriber into the SubscriberEvents DynamoDB table. The table’s composite primary key (subscriberId, timestamp) ensures efficient, timestamp-ordered queries.

Example:

```javascript
exports.handler = async (event) => {
  // Group Kinesis data by agentId
  const recordsByAgent = {};
  for (const record of event.Records) {
    const agentId = record.kinesis.partitionKey;
    const encodedData = record.kinesis.data;
    (recordsByAgent[agentId] ??= []).push(encodedData);
  }

  // Process each agent group
  for (const agentId in recordsByAgent) {
    // Fetch subscribers for this agent
    const subscribers = await queryDynamo(SUBSCRIBERS_TABLE, { agentId });
    if (!subscribers.length) continue;

    // Write an event for each subscriber and record
    for (const { subscriberId } of subscribers) {
      for (const dataStr of recordsByAgent[agentId]) {
        const writes = recordsByAgent[agentId].map(event => ({
          PutRequest: { Item: {
            subscriberId,
            timestamp: event.timestamp,
            data: event.data,
          } }
        }));

        await batchWriteSubscriberEvents(writes);
      }
    }
  }

  return { statusCode: 200 };
};
```

#### Publisher Lambda Function

Triggered by DynamoDB Streams on the SubscriberEvents table, this Lambda reads new items, groups them by subscriberId, sorts by timestamp, and sends ordered batches of messages to a dedicated SQS FIFO queue per subscriber. FIFO queues preserve message order and deduplicate via MessageDeduplicationId.

Example code:

```javascript
exports.handler = async (event) => {
  // 1. Group DynamoDB stream record keys by subscriberId
  const recordsBySubscriber = groupKeys(event.Records);

  // 2. For each subscriber:
  for (const subscriberId in recordsBySubscriber) {

    // a) Sort timestamps
    const timestamps = sort(recordsBySubscriber[subscriberId]);

    // b) Batch-read events from DynamoDB
    const events = batchGetItems(SUBSCRIER_EVENTS_TABLE, subscriberId, timestamps);

    // c) Send events to SQS FIFO in batches
    sendInBatches(events, subscriberId, process.env.DISPATCH_DELAY_SECONDS);
  }

  return { statusCode: 200 };
};
```

#### Agent Consumption

Each agent polls its own SQS FIFO queue for messages. FIFO semantics guarantee that messages are delivered in the exact order they were written, allowing agents to process events sequentially. Agents can use long polling to reduce costs and improve efficiency.

# API Reference

## 1. Register Agent

**Endpoint**

    POST /agents

**Description**

Create a new agent. Returns a unique `agentId`.
Creates a record in DynamoDB with the agentId and an empty list of subscriptions.

**Request**

    {}

**Response**

201 Created

    {
        "agentId": "unique-agent-id"
    }

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
- **Attributes**:
    - `createdAt` (string, ISO 8601 format): The timestamp when the agent was created.
    - `subscriptions` (list): A list of subscriptions for the agent, each containing `producerId`.

## SubscriberEvents Table

- **Table Name**: SubscriberEvents
- **Primary Key**:
  - Partition Key: `subcriberId` (string)
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
