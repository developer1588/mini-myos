import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import { KinesisStreamEvent } from "aws-lambda";

const client = new DynamoDBClient({});
const ddb = DynamoDBDocumentClient.from(client);

export const handler = async (event: KinesisStreamEvent) => {
  // 1) Group records by agentId (partitionKey)
  const groups: Record<string, Array<{ timestamp: string; data: any }>> = {};
  for (const {
    kinesis: { partitionKey, data },
  } of event.Records) {
    const decoded = JSON.parse(Buffer.from(data, "base64").toString("utf-8"));
    (groups[partitionKey] ??= []).push(decoded);
  }

  // 2) For each agent, lookup subscribers and write events
  for (const [agentId, records] of Object.entries(groups)) {
    const res = await ddb.send(
      new QueryCommand({
        TableName: process.env.SUBSCRIBERS_TABLE!,
        KeyConditionExpression: "producerId = :pid",
        ExpressionAttributeValues: { ":pid": agentId },
        ProjectionExpression: "subscriberId",
      }),
    );

    const subs = res.Items ?? [];
    if (subs.length === 0) continue;

    // 3) Batch‐write records per subscriber per event
    for (const { subscriberId } of subs) {
      const items = records.map(({ timestamp, data }) => ({
        PutRequest: {
          Item: { subscriberId, timestamp, data },
        },
      }));
      await ddb.send(
        new BatchWriteCommand({
          RequestItems: { [process.env.SUB_EVENTS_TABLE!]: items },
        }),
      );
    }
  }

  return { statusCode: 200 };
};
