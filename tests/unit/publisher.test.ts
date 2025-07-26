import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import { DynamoDBStreamEvent } from "aws-lambda";
import { mockClient } from "aws-sdk-client-mock";

import { handler } from "../../lambda/publisher";

// Create mocks for AWS clients
const dynamoMock = mockClient(DynamoDBClient);
const docClientMock = mockClient(DynamoDBDocumentClient);
const sqsMock = mockClient(SQSClient);

// Mock environment variables
const mockEnv = {
  SUB_EVENTS_TABLE: "test-sub-events-table",
  AGENTS_TABLE: "test-agents-table",
};

describe("Publisher Lambda Handler", () => {
  beforeEach(() => {
    // Reset all mocks
    dynamoMock.reset();
    docClientMock.reset();
    sqsMock.reset();
    jest.clearAllMocks();

    // Set environment variables
    Object.assign(process.env, mockEnv);

    // Suppress console.error during tests
    jest.spyOn(console, "error").mockImplementation(() => {});
    jest.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const createMockDynamoDBStreamEvent = (
    records: Array<{
      subscriberId: string;
      timestamp: string;
      data?: any;
    }>,
  ): DynamoDBStreamEvent => ({
    Records: records.map(({ subscriberId, timestamp, data }) => ({
      eventID: `event-${subscriberId}-${timestamp}`,
      eventName: "INSERT",
      eventVersion: "1.1",
      eventSource: "aws:dynamodb",
      awsRegion: "us-east-1",
      dynamodb: {
        Keys: {
          subscriberId: { S: subscriberId },
          timestamp: { S: timestamp },
        },
        NewImage: {
          subscriberId: { S: subscriberId },
          timestamp: { S: timestamp },
          ...(data && { data: { S: JSON.stringify(data) } }),
        },
        SequenceNumber: "123456789",
        SizeBytes: 100,
        StreamViewType: "NEW_AND_OLD_IMAGES",
      },
    })),
  });

  describe("Basic functionality", () => {
    it("should process single subscriber with single event", async () => {
      const subscriberId = "sub-1";
      const timestamp = "2023-01-01T00:00:00.000Z";
      const eventData = { message: "test" };

      // Mock SubEventsTable query
      docClientMock.on(QueryCommand).resolves({
        Items: [
          {
            subscriberId,
            timestamp,
            data: eventData,
          },
        ],
        Count: 1,
      });

      // Mock AgentsTable get
      docClientMock.on(GetCommand).resolves({
        Item: {
          agentId: subscriberId,
          queueUrl:
            "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        },
      });

      // Mock SQS send message
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: "msg-123",
      });

      const event = createMockDynamoDBStreamEvent([
        {
          subscriberId,
          timestamp,
          data: eventData,
        },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Verify SubEventsTable query
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(QueryCommand)[0].args[0].input).toEqual(
        {
          TableName: mockEnv.SUB_EVENTS_TABLE,
          KeyConditionExpression: "subscriberId = :subscriberId",
          ExpressionAttributeValues: { ":subscriberId": subscriberId },
          ScanIndexForward: false,
          Limit: 1,
        },
      );

      // Verify AgentsTable get
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(GetCommand)[0].args[0].input).toEqual({
        TableName: mockEnv.AGENTS_TABLE,
        Key: { agentId: subscriberId },
      });

      // Verify SQS send message
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(1);
      const sqsCall = sqsMock.commandCalls(SendMessageCommand)[0].args[0].input;
      expect(sqsCall.QueueUrl).toBe(
        "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
      );

      const messageBody = JSON.parse(sqsCall.MessageBody!);
      expect(messageBody).toEqual({
        subscriberId,
        events: [
          {
            subscriberId,
            timestamp,
            data: eventData,
          },
        ],
        timestamp: expect.any(String),
      });
    });

    it("should process multiple events for same subscriber", async () => {
      const subscriberId = "sub-1";
      const timestamp1 = "2023-01-01T00:00:00.000Z";
      const timestamp2 = "2023-01-01T00:01:00.000Z";
      const eventData1 = { message: "test1" };
      const eventData2 = { message: "test2" };

      // Mock SubEventsTable query
      docClientMock.on(QueryCommand).resolves({
        Items: [
          { subscriberId, timestamp: timestamp2, data: eventData2 }, // Latest first
          { subscriberId, timestamp: timestamp1, data: eventData1 },
        ],
        Count: 2,
      });

      // Mock AgentsTable get
      docClientMock.on(GetCommand).resolves({
        Item: {
          agentId: subscriberId,
          queueUrl:
            "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        },
      });

      // Mock SQS send message
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: "msg-123",
      });

      const event = createMockDynamoDBStreamEvent([
        { subscriberId, timestamp: timestamp1, data: eventData1 },
        { subscriberId, timestamp: timestamp2, data: eventData2 },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should only query once per subscriber
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(
        docClientMock.commandCalls(QueryCommand)[0].args[0].input.Limit,
      ).toBe(2);

      // Should only send one message per subscriber
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(1);

      const messageBody = JSON.parse(
        sqsMock.commandCalls(SendMessageCommand)[0].args[0].input.MessageBody!,
      );
      expect(messageBody.events).toHaveLength(2);
    });

    it("should process multiple subscribers independently", async () => {
      const subscriber1Id = "sub-1";
      const subscriber2Id = "sub-2";
      const timestamp = "2023-01-01T00:00:00.000Z";
      const eventData1 = { message: "test1" };
      const eventData2 = { message: "test2" };

      // Mock SubEventsTable queries for each subscriber
      docClientMock
        .on(QueryCommand, {
          TableName: mockEnv.SUB_EVENTS_TABLE,
          KeyConditionExpression: "subscriberId = :subscriberId",
          ExpressionAttributeValues: { ":subscriberId": subscriber1Id },
        })
        .resolves({
          Items: [{ subscriberId: subscriber1Id, timestamp, data: eventData1 }],
          Count: 1,
        });

      docClientMock
        .on(QueryCommand, {
          TableName: mockEnv.SUB_EVENTS_TABLE,
          KeyConditionExpression: "subscriberId = :subscriberId",
          ExpressionAttributeValues: { ":subscriberId": subscriber2Id },
        })
        .resolves({
          Items: [{ subscriberId: subscriber2Id, timestamp, data: eventData2 }],
          Count: 1,
        });

      // Mock AgentsTable gets
      docClientMock
        .on(GetCommand, {
          TableName: mockEnv.AGENTS_TABLE,
          Key: { agentId: subscriber1Id },
        })
        .resolves({
          Item: {
            agentId: subscriber1Id,
            queueUrl:
              "https://sqs.us-east-1.amazonaws.com/123456789012/queue-1",
          },
        });

      docClientMock
        .on(GetCommand, {
          TableName: mockEnv.AGENTS_TABLE,
          Key: { agentId: subscriber2Id },
        })
        .resolves({
          Item: {
            agentId: subscriber2Id,
            queueUrl:
              "https://sqs.us-east-1.amazonaws.com/123456789012/queue-2",
          },
        });

      // Mock SQS send messages
      sqsMock.on(SendMessageCommand).resolves({
        MessageId: "msg-123",
      });

      const event = createMockDynamoDBStreamEvent([
        { subscriberId: subscriber1Id, timestamp, data: eventData1 },
        { subscriberId: subscriber2Id, timestamp, data: eventData2 },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query each subscriber
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(2);

      // Should get each agent
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(2);

      // Should send message to each queue
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(2);
    });
  });

  describe("Edge cases", () => {
    it("should skip events that are not INSERT", async () => {
      const event: DynamoDBStreamEvent = {
        Records: [
          {
            eventID: "event-1",
            eventName: "REMOVE",
            eventVersion: "1.1",
            eventSource: "aws:dynamodb",
            awsRegion: "us-east-1",
            dynamodb: {
              Keys: {
                subscriberId: { S: "sub-1" },
                timestamp: { S: "2023-01-01T00:00:00.000Z" },
              },
              SequenceNumber: "123456789",
              SizeBytes: 100,
              StreamViewType: "NEW_AND_OLD_IMAGES",
            },
          },
        ],
      };

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should not perform any operations
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(0);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(0);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });

    it("should skip events without NewImage", async () => {
      const event: DynamoDBStreamEvent = {
        Records: [
          {
            eventID: "event-1",
            eventName: "INSERT",
            eventVersion: "1.1",
            eventSource: "aws:dynamodb",
            awsRegion: "us-east-1",
            dynamodb: {
              Keys: {
                subscriberId: { S: "sub-1" },
                timestamp: { S: "2023-01-01T00:00:00.000Z" },
              },
              SequenceNumber: "123456789",
              SizeBytes: 100,
              StreamViewType: "NEW_AND_OLD_IMAGES",
            },
          },
        ],
      };

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should not perform any operations
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(0);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(0);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });

    it("should skip events without subscriberId", async () => {
      const event: DynamoDBStreamEvent = {
        Records: [
          {
            eventID: "event-1",
            eventName: "INSERT",
            eventVersion: "1.1",
            eventSource: "aws:dynamodb",
            awsRegion: "us-east-1",
            dynamodb: {
              NewImage: {
                timestamp: { S: "2023-01-01T00:00:00.000Z" },
              },
              SequenceNumber: "123456789",
              SizeBytes: 100,
              StreamViewType: "NEW_AND_OLD_IMAGES",
            },
          },
        ],
      };

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should not perform any operations
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(0);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(0);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });

    it("should handle subscriber with no events", async () => {
      const subscriberId = "sub-1";
      const timestamp = "2023-01-01T00:00:00.000Z";

      // Mock SubEventsTable query returning no events
      docClientMock.on(QueryCommand).resolves({
        Items: [],
        Count: 0,
      });

      const event = createMockDynamoDBStreamEvent([
        {
          subscriberId,
          timestamp,
        },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query but not proceed further
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(0);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });

    it("should handle agent with no queue URL", async () => {
      const subscriberId = "sub-1";
      const timestamp = "2023-01-01T00:00:00.000Z";

      // Mock SubEventsTable query
      docClientMock.on(QueryCommand).resolves({
        Items: [{ subscriberId, timestamp }],
        Count: 1,
      });

      // Mock AgentsTable get returning agent without queueUrl
      docClientMock.on(GetCommand).resolves({
        Item: {
          agentId: subscriberId,
          // Missing queueUrl
        },
      });

      const event = createMockDynamoDBStreamEvent([
        {
          subscriberId,
          timestamp,
        },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query and get agent but not send message
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(1);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });

    it("should handle empty DynamoDB stream event", async () => {
      const event: DynamoDBStreamEvent = { Records: [] };

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should not perform any operations
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(0);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(0);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });
  });

  describe("Error handling", () => {
    it("should handle SubEventsTable query errors", async () => {
      const subscriberId = "sub-1";
      const timestamp = "2023-01-01T00:00:00.000Z";

      docClientMock
        .on(QueryCommand)
        .rejects(new Error("SubEventsTable query error"));

      const event = createMockDynamoDBStreamEvent([
        {
          subscriberId,
          timestamp,
        },
      ]);

      await expect(handler(event)).rejects.toThrow(
        "SubEventsTable query error",
      );

      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(0);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });

    it("should handle AgentsTable get errors", async () => {
      const subscriberId = "sub-1";
      const timestamp = "2023-01-01T00:00:00.000Z";

      docClientMock.on(QueryCommand).resolves({
        Items: [{ subscriberId, timestamp }],
        Count: 1,
      });

      docClientMock.on(GetCommand).rejects(new Error("AgentsTable get error"));

      const event = createMockDynamoDBStreamEvent([
        {
          subscriberId,
          timestamp,
        },
      ]);

      await expect(handler(event)).rejects.toThrow("AgentsTable get error");

      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(1);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    });

    it("should handle SQS send message errors", async () => {
      const subscriberId = "sub-1";
      const timestamp = "2023-01-01T00:00:00.000Z";

      docClientMock.on(QueryCommand).resolves({
        Items: [{ subscriberId, timestamp }],
        Count: 1,
      });

      docClientMock.on(GetCommand).resolves({
        Item: {
          agentId: subscriberId,
          queueUrl:
            "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        },
      });

      sqsMock.on(SendMessageCommand).rejects(new Error("SQS send error"));

      const event = createMockDynamoDBStreamEvent([
        {
          subscriberId,
          timestamp,
        },
      ]);

      await expect(handler(event)).rejects.toThrow("SQS send error");

      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(GetCommand)).toHaveLength(1);
      expect(sqsMock.commandCalls(SendMessageCommand)).toHaveLength(1);
    });
  });

  describe("Data batching", () => {
    it("should correctly batch events by subscriber", async () => {
      const subscriber1Id = "sub-1";
      const subscriber2Id = "sub-2";
      const timestamp1 = "2023-01-01T00:00:00.000Z";
      const timestamp2 = "2023-01-01T00:01:00.000Z";
      const timestamp3 = "2023-01-01T00:02:00.000Z";

      // Mock queries for each subscriber
      docClientMock
        .on(QueryCommand, {
          TableName: mockEnv.SUB_EVENTS_TABLE,
          ExpressionAttributeValues: { ":subscriberId": subscriber1Id },
          Limit: 2,
        })
        .resolves({
          Items: [
            { subscriberId: subscriber1Id, timestamp: timestamp2 },
            { subscriberId: subscriber1Id, timestamp: timestamp1 },
          ],
          Count: 2,
        });

      docClientMock
        .on(QueryCommand, {
          TableName: mockEnv.SUB_EVENTS_TABLE,
          ExpressionAttributeValues: { ":subscriberId": subscriber2Id },
          Limit: 1,
        })
        .resolves({
          Items: [{ subscriberId: subscriber2Id, timestamp: timestamp3 }],
          Count: 1,
        });

      // Mock agent gets
      docClientMock.on(GetCommand).resolves({
        Item: {
          agentId: "test-agent",
          queueUrl:
            "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        },
      });

      sqsMock.on(SendMessageCommand).resolves({
        MessageId: "msg-123",
      });

      const event = createMockDynamoDBStreamEvent([
        { subscriberId: subscriber1Id, timestamp: timestamp1 },
        { subscriberId: subscriber2Id, timestamp: timestamp3 },
        { subscriberId: subscriber1Id, timestamp: timestamp2 },
      ]);

      const result = await handler(event);

      expect(result.statusCode).toBe(200);

      // Should query with correct limits based on event count per subscriber
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(2);

      const sub1Query = docClientMock
        .commandCalls(QueryCommand)
        .find(
          (call) =>
            call.args[0].input.ExpressionAttributeValues?.[":subscriberId"] ===
            subscriber1Id,
        );
      expect(sub1Query).toBeDefined();
      expect(sub1Query!.args[0].input.Limit).toBe(2);

      const sub2Query = docClientMock
        .commandCalls(QueryCommand)
        .find(
          (call) =>
            call.args[0].input.ExpressionAttributeValues?.[":subscriberId"] ===
            subscriber2Id,
        );
      expect(sub2Query).toBeDefined();
      expect(sub2Query!.args[0].input.Limit).toBe(1);
    });
  });
});
