import {
  CloudFormationClient,
  DescribeStackResourcesCommand,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import {
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  ScanCommand,
} from "@aws-sdk/client-dynamodb";
import {
  GetRecordsCommand,
  GetShardIteratorCommand,
  KinesisClient,
  ListShardsCommand,
  PutRecordCommand,
} from "@aws-sdk/client-kinesis";
import {
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import axios from "axios";

// AWS clients
export const cf = new CloudFormationClient({
  region: process.env.AWS_REGION,
  endpoint: process.env.LOCALSTACK_ENDPOINT,
});

export const ddb = new DynamoDBClient({
  region: process.env.AWS_REGION,
  endpoint: process.env.LOCALSTACK_ENDPOINT,
});

export const sqs = new SQSClient({
  region: process.env.AWS_REGION,
  endpoint: process.env.LOCALSTACK_ENDPOINT,
});

export const kinesis = new KinesisClient({
  region: process.env.AWS_REGION,
  endpoint: process.env.LOCALSTACK_ENDPOINT,
});

// Types
export interface TableNames {
  subscribersTable: string;
  subEventsTable: string;
  agentsTable: string;
}

export interface Agent {
  agentId: string;
  queueUrl?: string;
  resourceArn: string;
}

// CloudFormation helpers
export async function getStackOutput(outputKey: string): Promise<string> {
  const resp = await cf.send(new DescribeStacksCommand({ StackName: "MyOS" }));
  const outputs = resp.Stacks?.[0].Outputs || [];
  const output = outputs.find((o) => o.OutputKey === outputKey)?.OutputValue;
  if (!output) throw new Error(`${outputKey} output not found`);

  return outputKey === "ApiUrl" && !output.endsWith("/")
    ? output + "/"
    : output;
}

export async function getApiUrl(): Promise<string> {
  return getStackOutput("ApiUrl");
}

export async function getStreamName(): Promise<string> {
  return getStackOutput("StreamName");
}

export async function getTableNames(): Promise<TableNames> {
  const resourcesResp = await cf.send(
    new DescribeStackResourcesCommand({ StackName: "MyOS" }),
  );

  const subsTableResource = resourcesResp.StackResources?.find(
    (r) =>
      r.LogicalResourceId?.startsWith("SubscribersTable") &&
      r.ResourceType === "AWS::DynamoDB::Table",
  );

  const subEventsTableResource = resourcesResp.StackResources?.find(
    (r) =>
      r.LogicalResourceId?.startsWith("SubEventsTable") &&
      r.ResourceType === "AWS::DynamoDB::Table",
  );

  const agentsTableResource = resourcesResp.StackResources?.find(
    (r) =>
      r.LogicalResourceId?.startsWith("AgentsTable") &&
      r.ResourceType === "AWS::DynamoDB::Table",
  );

  if (
    !subsTableResource?.PhysicalResourceId ||
    !subEventsTableResource?.PhysicalResourceId ||
    !agentsTableResource?.PhysicalResourceId
  ) {
    throw new Error("Required tables not found in stack resources");
  }

  return {
    subscribersTable: subsTableResource.PhysicalResourceId,
    subEventsTable: subEventsTableResource.PhysicalResourceId,
    agentsTable: agentsTableResource.PhysicalResourceId,
  };
}

// Agent management
export async function registerAgent(
  apiUrl: string,
  resourceArn: string,
): Promise<Agent> {
  const response = await axios.post(apiUrl + "agents", { resourceArn });
  return response.data;
}

// Subscription management
export async function createSubscription(
  apiUrl: string,
  listenerId: string,
  producerId: string,
): Promise<void> {
  await axios.put(
    apiUrl + `agents/${listenerId}/subscriptions/${producerId}`,
    {},
    { headers: { "Content-Type": "application/json" } },
  );
}

export async function verifySubscription(
  tableName: string,
  producerId: string,
  subscriberId: string,
): Promise<boolean> {
  try {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: tableName,
        Key: {
          producerId: { S: producerId },
          subscriberId: { S: subscriberId },
        },
      }),
    );
    return !!result.Item;
  } catch {
    return false;
  }
}

// DynamoDB helpers
export async function cleanDynamoDBTable(tableName: string): Promise<void> {
  try {
    const result = await ddb.send(new ScanCommand({ TableName: tableName }));
    const items = result.Items || [];

    for (const item of items) {
      const key: any = {};

      // Handle different table schemas
      if (tableName.includes("AgentsTable")) {
        if (item.agentId) key.agentId = item.agentId;
      } else if (tableName.includes("SubscribersTable")) {
        if (item.producerId) key.producerId = item.producerId;
        if (item.subscriberId) key.subscriberId = item.subscriberId;
      } else if (tableName.includes("SubEventsTable")) {
        if (item.subscriberId) key.subscriberId = item.subscriberId;
        if (item.timestamp) key.timestamp = item.timestamp;
      }

      // Fallback for generic keys
      if (Object.keys(key).length === 0) {
        if (item.subscriberId) key.subscriberId = item.subscriberId;
        if (item.timestamp) key.timestamp = item.timestamp;
        if (item.producerId) key.producerId = item.producerId;
        if (item.listenerId) key.listenerId = item.listenerId;
        if (item.agentId) key.agentId = item.agentId;
      }

      await ddb.send(
        new DeleteItemCommand({
          TableName: tableName,
          Key: key,
        }),
      );
    }
  } catch (error) {
    console.warn(`Failed to clean table ${tableName}:`, error);
  }
}

export async function addSubEvent(
  subEventsTable: string,
  subscriberId: string,
  timestamp: string,
  data: any,
): Promise<void> {
  await ddb.send(
    new PutItemCommand({
      TableName: subEventsTable,
      Item: {
        subscriberId: { S: subscriberId },
        timestamp: { S: timestamp },
        data: {
          M: {
            message: { S: data.message },
            eventType: { S: data.eventType },
          },
        },
      },
    }),
  );
}

export async function waitForEvents(
  subEventsTable: string,
  expectedCount: number,
  timeoutMs: number = 10000,
): Promise<any[]> {
  const startTime = Date.now();

  while (Date.now() - startTime < timeoutMs) {
    const result = await ddb.send(
      new ScanCommand({
        TableName: subEventsTable,
      }),
    );

    const items = result.Items || [];
    if (items.length >= expectedCount) {
      return items;
    }

    // Wait 500ms before checking again
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  throw new Error(
    `Timeout waiting for ${expectedCount} events in subEventsTable`,
  );
}

export async function purgeQueue(queueUrl: string): Promise<void> {
  try {
    await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
    // Wait a bit for purge to complete
    await new Promise((resolve) => setTimeout(resolve, 1000));
  } catch (error) {
    console.warn(`Failed to purge queue ${queueUrl}:`, error);
  }
}

export async function waitForSQSMessages(
  queueUrl: string,
  expectedCount: number,
  timeoutMs: number = 10000,
): Promise<any[]> {
  const startTime = Date.now();
  const messages: any[] = [];

  while (
    Date.now() - startTime < timeoutMs &&
    messages.length < expectedCount
  ) {
    const result = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
      }),
    );

    if (result.Messages) {
      for (const message of result.Messages) {
        if (message.Body) {
          messages.push(JSON.parse(message.Body));
        }
      }
    }

    if (messages.length < expectedCount) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  return messages;
}

// Kinesis helpers
export async function sendEventToKinesis(
  streamName: string,
  event: any,
): Promise<void> {
  await kinesis.send(
    new PutRecordCommand({
      StreamName: streamName,
      Data: Buffer.from(JSON.stringify(event)),
      PartitionKey: event.producerId || "default",
    }),
  );
}

export async function cleanKinesisStream(streamName: string): Promise<void> {
  try {
    const shardsResult = await kinesis.send(
      new ListShardsCommand({ StreamName: streamName }),
    );
    const shards = shardsResult.Shards || [];

    for (const shard of shards) {
      if (!shard.ShardId) continue;

      // Get iterator for the shard starting from LATEST to consume new records
      const iteratorResult = await kinesis.send(
        new GetShardIteratorCommand({
          StreamName: streamName,
          ShardId: shard.ShardId,
          ShardIteratorType: "LATEST",
        }),
      );

      let shardIterator = iteratorResult.ShardIterator;

      // Consume records until no more
      while (shardIterator) {
        const recordsResult = await kinesis.send(
          new GetRecordsCommand({
            ShardIterator: shardIterator,
          }),
        );

        shardIterator = recordsResult.NextShardIterator;

        // If no records, break
        if (!recordsResult.Records || recordsResult.Records.length === 0) {
          break;
        }

        // Small delay to avoid hitting limits
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  } catch (error) {
    console.warn(`Failed to clean Kinesis stream ${streamName}:`, error);
  }
}

/**
 * Waits for records on a Kinesis stream matching the given partitionKey.
 */
export async function waitForKinesisStream(
  streamName: string,
  partitionKey: string,
  expectedCount: number,
  timeoutMs: number = 10000,
): Promise<any[]> {
  const startTime = Date.now();
  const records: any[] = [];

  // Get all shards
  const { Shards = [] } = await kinesis.send(
    new ListShardsCommand({ StreamName: streamName }),
  );

  // Get LATEST iterator for each shard
  const iterators = await Promise.all(
    Shards.map(async (shard) => {
      const result = await kinesis.send(
        new GetShardIteratorCommand({
          StreamName: streamName,
          ShardId: shard.ShardId!,
          ShardIteratorType: "LATEST",
        }),
      );
      return result.ShardIterator!;
    }),
  );

  let currentIterators = [...iterators];

  while (Date.now() - startTime < timeoutMs && records.length < expectedCount) {
    // Read from all shards
    for (let i = 0; i < currentIterators.length; i++) {
      if (!currentIterators[i]) continue;

      const result = await kinesis.send(
        new GetRecordsCommand({
          ShardIterator: currentIterators[i],
          Limit: 10,
        }),
      );

      currentIterators[i] = result.NextShardIterator!;

      for (const record of result.Records || []) {
        if (record.PartitionKey === partitionKey) {
          try {
            const data = JSON.parse(
              Buffer.from(record.Data as Uint8Array).toString(),
            );
            records.push(data);
          } catch {
            records.push(record);
          }
        }
      }
    }

    if (records.length < expectedCount) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  return records;
}
