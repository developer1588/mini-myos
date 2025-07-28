import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import { KinesisStreamEvent } from "aws-lambda";
import { mockClient } from "aws-sdk-client-mock";

import { handler } from "../../lambda/aggregator";

// Create mocks for AWS clients
const dynamoMock = mockClient(DynamoDBClient);
const docClientMock = mockClient(DynamoDBDocumentClient);

// Mock environment variables
const mockEnv = {
  SUBSCRIBERS_TABLE: "test-subscribers-table",
  SUB_EVENTS_TABLE: "test-sub-events-table",
};

describe("Aggregator Lambda Handler", () => {
  beforeEach(() => {
    // Reset all mocks
    dynamoMock.reset();
    docClientMock.reset();
    jest.clearAllMocks();

    // Set environment variables
    Object.assign(process.env, mockEnv);

    // Suppress console.error during tests
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const createMockKinesisEvent = (
    records: Array<{
      partitionKey: string;
      data: { timestamp: string; data: any };
    }>,
  ): KinesisStreamEvent => ({
    Records: records.map(
      ({ partitionKey, data }) =>
        ({
          kinesis: {
            partitionKey,
            data: Buffer.from(JSON.stringify(data)).toString("base64"),
            sequenceNumber: "123456789",
            approximateArrivalTimestamp: 1234567890,
          },
          eventSource: "aws:kinesis",
          eventVersion: "1.0",
          eventID: "shardId-000000000000:123456789",
          eventName: "aws:kinesis:record",
          invokeIdentityArn: "arn:aws:iam::123456789012:role/test-role",
          awsRegion: "us-east-1",
          eventSourceARN:
            "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
        }) as any,
    ),
  });

  describe("Basic functionality", () => {
    it("should process single record with single subscriber", async () => {
      const agentId = "agent-1";
      const eventData = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test" },
      };
      const subscriber = { subscriberId: "sub-1" };

      // Mock subscriber query
      docClientMock.on(QueryCommand).resolves({
        Items: [subscriber],
        Count: 1,
      });

      // Mock batch write
      docClientMock.on(BatchWriteCommand).resolves({});

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: eventData },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Verify subscriber query
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(QueryCommand)[0].args[0].input).toEqual(
        {
          TableName: mockEnv.SUBSCRIBERS_TABLE,
          KeyConditionExpression: "producerId = :pid",
          ExpressionAttributeValues: { ":pid": agentId },
          ProjectionExpression: "subscriberId",
        },
      );

      // Verify batch write
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(1);
      expect(
        docClientMock.commandCalls(BatchWriteCommand)[0].args[0].input,
      ).toEqual({
        RequestItems: {
          [mockEnv.SUB_EVENTS_TABLE]: [
            {
              PutRequest: {
                Item: {
                  subscriberId: "sub-1",
                  timestamp: eventData.timestamp,
                  data: eventData.data,
                },
              },
            },
          ],
        },
      });
    });

    it("should process multiple records for same agent", async () => {
      const agentId = "agent-1";
      const eventData1 = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test1" },
      };
      const eventData2 = {
        timestamp: "2023-01-01T00:01:00.000Z",
        data: { message: "test2" },
      };
      const subscriber = { subscriberId: "sub-1" };

      docClientMock.on(QueryCommand).resolves({
        Items: [subscriber],
        Count: 1,
      });
      docClientMock.on(BatchWriteCommand).resolves({});

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: eventData1 },
        { partitionKey: agentId, data: eventData2 },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should only query subscribers once per agent
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);

      // Should batch write both events
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(1);
      const batchWriteCall =
        docClientMock.commandCalls(BatchWriteCommand)[0].args[0].input;
      expect(
        batchWriteCall.RequestItems?.[mockEnv.SUB_EVENTS_TABLE],
      ).toHaveLength(2);
      expect(
        batchWriteCall.RequestItems?.[mockEnv.SUB_EVENTS_TABLE]?.[0]?.PutRequest
          ?.Item,
      ).toEqual({
        subscriberId: "sub-1",
        timestamp: eventData1.timestamp,
        data: eventData1.data,
      });
      expect(
        batchWriteCall.RequestItems?.[mockEnv.SUB_EVENTS_TABLE]?.[1]?.PutRequest
          ?.Item,
      ).toEqual({
        subscriberId: "sub-1",
        timestamp: eventData2.timestamp,
        data: eventData2.data,
      });
    });

    it("should process multiple agents with different subscribers", async () => {
      const agent1Id = "agent-1";
      const agent2Id = "agent-2";
      const eventData1 = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test1" },
      };
      const eventData2 = {
        timestamp: "2023-01-01T00:01:00.000Z",
        data: { message: "test2" },
      };

      // Mock different subscribers for each agent
      docClientMock
        .on(QueryCommand, {
          TableName: mockEnv.SUBSCRIBERS_TABLE,
          KeyConditionExpression: "producerId = :pid",
          ExpressionAttributeValues: { ":pid": agent1Id },
        })
        .resolves({
          Items: [{ subscriberId: "sub-1" }],
          Count: 1,
        });

      docClientMock
        .on(QueryCommand, {
          TableName: mockEnv.SUBSCRIBERS_TABLE,
          KeyConditionExpression: "producerId = :pid",
          ExpressionAttributeValues: { ":pid": agent2Id },
        })
        .resolves({
          Items: [{ subscriberId: "sub-2" }],
          Count: 1,
        });

      docClientMock.on(BatchWriteCommand).resolves({});

      const event = createMockKinesisEvent([
        { partitionKey: agent1Id, data: eventData1 },
        { partitionKey: agent2Id, data: eventData2 },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query subscribers for each agent
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(2);

      // Should batch write for each subscriber
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(2);
    });

    it("should handle multiple subscribers for single agent", async () => {
      const agentId = "agent-1";
      const eventData = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test" },
      };
      const subscribers = [
        { subscriberId: "sub-1" },
        { subscriberId: "sub-2" },
        { subscriberId: "sub-3" },
      ];

      docClientMock.on(QueryCommand).resolves({
        Items: subscribers,
        Count: 3,
      });
      docClientMock.on(BatchWriteCommand).resolves({});

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: eventData },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query subscribers once
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);

      // Should batch write for each subscriber
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(3);

      // Verify each subscriber gets the event
      const batchWrites = docClientMock.commandCalls(BatchWriteCommand);
      expect(
        batchWrites[0]?.args[0]?.input?.RequestItems?.[
          mockEnv.SUB_EVENTS_TABLE
        ]?.[0]?.PutRequest?.Item?.subscriberId,
      ).toBe("sub-1");
      expect(
        batchWrites[1]?.args[0]?.input?.RequestItems?.[
          mockEnv.SUB_EVENTS_TABLE
        ]?.[0]?.PutRequest?.Item?.subscriberId,
      ).toBe("sub-2");
      expect(
        batchWrites[2]?.args[0]?.input?.RequestItems?.[
          mockEnv.SUB_EVENTS_TABLE
        ]?.[0]?.PutRequest?.Item?.subscriberId,
      ).toBe("sub-3");
    });
  });

  describe("Edge cases", () => {
    it("should skip agents with no subscribers", async () => {
      const agentId = "agent-1";
      const eventData = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test" },
      };

      docClientMock.on(QueryCommand).resolves({
        Items: [],
        Count: 0,
      });

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: eventData },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query subscribers
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);

      // Should not perform any batch writes
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });

    it("should handle undefined subscribers list", async () => {
      const agentId = "agent-1";
      const eventData = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test" },
      };

      docClientMock.on(QueryCommand).resolves({
        Items: undefined,
        Count: 0,
      });

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: eventData },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query subscribers
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);

      // Should not perform any batch writes
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });

    it("should handle empty Kinesis event", async () => {
      const event: KinesisStreamEvent = { Records: [] };

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should not perform any operations
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(0);
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });

    it("should handle complex event data structures", async () => {
      const agentId = "agent-1";
      const complexEventData = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: {
          nested: {
            object: {
              with: ["arrays", "and", "values"],
              numbers: 42,
              boolean: true,
              nullValue: null,
            },
          },
          topLevel: "string",
        },
      };
      const subscriber = { subscriberId: "sub-1" };

      docClientMock.on(QueryCommand).resolves({
        Items: [subscriber],
        Count: 1,
      });
      docClientMock.on(BatchWriteCommand).resolves({});

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: complexEventData },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      const batchWriteCall =
        docClientMock.commandCalls(BatchWriteCommand)[0].args[0].input;
      expect(
        batchWriteCall.RequestItems?.[mockEnv.SUB_EVENTS_TABLE]?.[0]?.PutRequest
          ?.Item,
      ).toEqual({
        subscriberId: "sub-1",
        timestamp: complexEventData.timestamp,
        data: complexEventData.data,
      });
    });
  });

  describe("Error handling", () => {
    it("should handle DynamoDB query errors", async () => {
      const agentId = "agent-1";
      const eventData = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test" },
      };

      docClientMock.on(QueryCommand).rejects(new Error("DynamoDB query error"));

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: eventData },
      ]);

      await expect(handler(event)).rejects.toThrow("DynamoDB query error");

      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });

    it("should handle DynamoDB batch write errors", async () => {
      const agentId = "agent-1";
      const eventData = {
        timestamp: "2023-01-01T00:00:00.000Z",
        data: { message: "test" },
      };
      const subscriber = { subscriberId: "sub-1" };

      docClientMock.on(QueryCommand).resolves({
        Items: [subscriber],
        Count: 1,
      });
      docClientMock
        .on(BatchWriteCommand)
        .rejects(new Error("DynamoDB batch write error"));

      const event = createMockKinesisEvent([
        { partitionKey: agentId, data: eventData },
      ]);

      await expect(handler(event)).rejects.toThrow(
        "DynamoDB batch write error",
      );

      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(1);
    });

    it("should handle malformed Kinesis data", async () => {
      const event: KinesisStreamEvent = {
        Records: [
          {
            kinesis: {
              partitionKey: "agent-1",
              data: "invalid-base64-json",
              sequenceNumber: "123456789",
              approximateArrivalTimestamp: 1234567890,
              kinesisSchemaVersion: "1.0",
            },
            eventSource: "aws:kinesis",
            eventVersion: "1.0",
            eventID: "shardId-000000000000:123456789",
            eventName: "aws:kinesis:record",
            invokeIdentityArn: "arn:aws:iam::123456789012:role/test-role",
            awsRegion: "us-east-1",
            eventSourceARN:
              "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
          },
        ],
      };

      await expect(handler(event)).rejects.toThrow();

      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(0);
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(0);
    });
  });

  describe("Data grouping", () => {
    it("should correctly group multiple records by partition key", async () => {
      const agent1Events = [
        {
          timestamp: "2023-01-01T00:00:00.000Z",
          data: { message: "agent1-event1" },
        },
        {
          timestamp: "2023-01-01T00:01:00.000Z",
          data: { message: "agent1-event2" },
        },
      ];
      const agent2Events = [
        {
          timestamp: "2023-01-01T00:02:00.000Z",
          data: { message: "agent2-event1" },
        },
      ];

      docClientMock.on(QueryCommand).resolves({
        Items: [{ subscriberId: "sub-1" }],
        Count: 1,
      });
      docClientMock.on(BatchWriteCommand).resolves({});

      const event = createMockKinesisEvent([
        { partitionKey: "agent-1", data: agent1Events[0] },
        { partitionKey: "agent-2", data: agent2Events[0] },
        { partitionKey: "agent-1", data: agent1Events[1] },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query subscribers for each unique agent
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(2);

      // Should batch write for each agent
      expect(docClientMock.commandCalls(BatchWriteCommand)).toHaveLength(2);
      // Verify agent-1 gets both events
      const agent1BatchWrite = docClientMock
        .commandCalls(BatchWriteCommand)
        .find(
          (call) =>
            call.args[0]?.input?.RequestItems?.[mockEnv.SUB_EVENTS_TABLE]
              ?.length === 2,
        );
      expect(agent1BatchWrite).toBeDefined();

      // Verify agent-2 gets one event
      const agent2BatchWrite = docClientMock
        .commandCalls(BatchWriteCommand)
        .find(
          (call) =>
            call.args[0]?.input?.RequestItems?.[mockEnv.SUB_EVENTS_TABLE]
              ?.length === 1,
        );
      expect(agent2BatchWrite).toBeDefined();
      expect(agent2BatchWrite).toBeDefined();
    });
  });
});
