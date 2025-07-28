import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  CreateQueueCommand,
  SetQueueAttributesCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import { APIGatewayProxyEvent } from "aws-lambda";
import { mockClient } from "aws-sdk-client-mock";
import { v4 as uuidv4 } from "uuid";

import { handler } from "../../lambda/register";

// Mock UUID
jest.mock("uuid", () => ({
  v4: jest.fn(),
}));
const mockUuidv4 = uuidv4 as jest.MockedFunction<() => string>;

// Create mocks for AWS clients
const dynamoMock = mockClient(DynamoDBClient);
const docClientMock = mockClient(DynamoDBDocumentClient);
const sqsMock = mockClient(SQSClient);

// Mock environment variables
const mockEnv = {
  AGENTS_TABLE: "test-agents-table",
  AWS_REGION: "us-east-1",
  AWS_ACCOUNT_ID: "123456789012",
};

describe("Register Lambda Handler", () => {
  beforeEach(() => {
    // Reset all mocks
    dynamoMock.reset();
    docClientMock.reset();
    sqsMock.reset();
    jest.clearAllMocks();

    // Set environment variables
    Object.assign(process.env, mockEnv);

    // Mock UUID generation
    mockUuidv4.mockReturnValue("test-uuid-1234");

    // Suppress console.error during tests
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const createMockEvent = (body: any): APIGatewayProxyEvent => ({
    body: JSON.stringify(body),
    headers: {},
    multiValueHeaders: {},
    httpMethod: "POST",
    isBase64Encoded: false,
    path: "/register",
    pathParameters: null,
    queryStringParameters: null,
    multiValueQueryStringParameters: null,
    stageVariables: null,
    requestContext: {} as any,
    resource: "",
  });

  describe("Input validation", () => {
    it("should return 400 when resourceArn is missing", async () => {
      const event = createMockEvent({});

      const result = await handler(event);

      expect(result.statusCode).toBe(400);
      expect(JSON.parse(result.body)).toEqual({
        error: "resourceArn is required",
      });
    });

    it("should return 400 when body is empty", async () => {
      const event = { ...createMockEvent({}), body: null };

      const result = await handler(event);

      expect(result.statusCode).toBe(400);
      expect(JSON.parse(result.body)).toEqual({
        error: "resourceArn is required",
      });
    });

    it("should return 400 when resourceArn is empty string", async () => {
      const event = createMockEvent({ resourceArn: "" });

      const result = await handler(event);

      expect(result.statusCode).toBe(400);
      expect(JSON.parse(result.body)).toEqual({
        error: "resourceArn is required",
      });
    });
  });

  describe("Existing agent scenario", () => {
    it("should return existing agent when resourceArn already exists", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";
      const existingAgent = {
        agentId: "existing-agent-id",
        queueUrl:
          "https://sqs.us-east-1.amazonaws.com/123456789012/existing-queue.fifo",
        resourceArn,
        createdAt: "2023-01-01T00:00:00.000Z",
      };

      docClientMock.on(QueryCommand).resolves({
        Items: [existingAgent],
        Count: 1,
      });

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(200);
      expect(JSON.parse(result.body)).toEqual({
        agentId: existingAgent.agentId,
        queueUrl: existingAgent.queueUrl,
        resourceArn: existingAgent.resourceArn,
      });

      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);
      expect(docClientMock.commandCalls(QueryCommand)[0].args[0].input).toEqual(
        {
          TableName: mockEnv.AGENTS_TABLE,
          IndexName: "resourceArn-index",
          KeyConditionExpression: "resourceArn = :resourceArn",
          ExpressionAttributeValues: {
            ":resourceArn": resourceArn,
          },
        },
      );

      // Should not create queue or save new agent
      expect(sqsMock.commandCalls(CreateQueueCommand)).toHaveLength(0);
      expect(docClientMock.commandCalls(PutCommand)).toHaveLength(0);
    });
  });

  describe("New agent creation", () => {
    it("should create new agent when resourceArn does not exist", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:new-function";
      const queueUrl =
        "https://sqs.us-east-1.amazonaws.com/123456789012/agents-test-uuid-1234.fifo";

      // Mock empty query result (no existing agent)
      docClientMock.on(QueryCommand).resolves({
        Items: [],
        Count: 0,
      });

      // Mock SQS queue creation
      sqsMock.on(CreateQueueCommand).resolves({
        QueueUrl: queueUrl,
      });

      // Mock SQS policy setting
      sqsMock.on(SetQueueAttributesCommand).resolves({});

      // Mock DynamoDB put
      docClientMock.on(PutCommand).resolves({});

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(200);
      expect(JSON.parse(result.body)).toEqual({
        agentId: "test-uuid-1234",
        queueUrl,
        resourceArn,
      });

      // Verify query was called
      expect(docClientMock.commandCalls(QueryCommand)).toHaveLength(1);

      // Verify SQS queue creation
      expect(sqsMock.commandCalls(CreateQueueCommand)).toHaveLength(1);
      expect(sqsMock.commandCalls(CreateQueueCommand)[0].args[0].input).toEqual(
        {
          QueueName: "agents-test-uuid-1234.fifo",
          Attributes: {
            FifoQueue: "true",
            ContentBasedDeduplication: "true",
          },
        },
      );

      // Verify SQS policy setting
      expect(sqsMock.commandCalls(SetQueueAttributesCommand)).toHaveLength(1);
      const policyCall = sqsMock.commandCalls(SetQueueAttributesCommand)[0]
        .args[0].input;
      expect(policyCall.QueueUrl).toBe(queueUrl);
      expect(policyCall.Attributes?.Policy).toBeDefined();

      const policy = JSON.parse(policyCall.Attributes!.Policy!);
      expect(policy.Statement[0].Principal.AWS).toBe(resourceArn);
      expect(policy.Statement[0].Action).toEqual([
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl",
      ]);

      // Verify DynamoDB put
      expect(docClientMock.commandCalls(PutCommand)).toHaveLength(1);
      const putCall = docClientMock.commandCalls(PutCommand)[0].args[0].input;
      expect(putCall.TableName).toBe(mockEnv.AGENTS_TABLE);
      expect(putCall.Item).toEqual({
        resourceArn,
        agentId: "test-uuid-1234",
        queueUrl,
        createdAt: expect.any(String),
      });
    });

    it("should handle queue policy with correct ARN format", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";

      docClientMock.on(QueryCommand).resolves({ Items: [], Count: 0 });
      sqsMock.on(CreateQueueCommand).resolves({
        QueueUrl:
          "https://sqs.us-east-1.amazonaws.com/123456789012/agents-test-uuid-1234.fifo",
      });
      sqsMock.on(SetQueueAttributesCommand).resolves({});
      docClientMock.on(PutCommand).resolves({});

      const event = createMockEvent({ resourceArn });
      await handler(event);

      const policyCall = sqsMock.commandCalls(SetQueueAttributesCommand)[0]
        .args[0].input;
      const policy = JSON.parse(policyCall.Attributes!.Policy!);

      expect(policy.Statement[0].Resource).toBe(
        `arn:aws:sqs:${mockEnv.AWS_REGION}:${mockEnv.AWS_ACCOUNT_ID}:agents-test-uuid-1234.fifo`,
      );
    });
  });

  describe("Error handling", () => {
    it("should return 500 when DynamoDB query fails", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";

      docClientMock.on(QueryCommand).rejects(new Error("DynamoDB error"));

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(500);
      expect(JSON.parse(result.body)).toEqual({
        error: "Internal server error",
      });
    });

    it("should return 500 when SQS queue creation fails", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";

      docClientMock.on(QueryCommand).resolves({ Items: [], Count: 0 });
      sqsMock.on(CreateQueueCommand).rejects(new Error("SQS error"));

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(500);
      expect(JSON.parse(result.body)).toEqual({
        error: "Internal server error",
      });
    });

    it("should return 500 when SQS policy setting fails", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";

      docClientMock.on(QueryCommand).resolves({ Items: [], Count: 0 });
      sqsMock.on(CreateQueueCommand).resolves({
        QueueUrl:
          "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue.fifo",
      });
      sqsMock.on(SetQueueAttributesCommand).rejects(new Error("Policy error"));

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(500);
      expect(JSON.parse(result.body)).toEqual({
        error: "Internal server error",
      });
    });

    it("should return 500 when DynamoDB put fails", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";

      docClientMock.on(QueryCommand).resolves({ Items: [], Count: 0 });
      sqsMock.on(CreateQueueCommand).resolves({
        QueueUrl:
          "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue.fifo",
      });
      sqsMock.on(SetQueueAttributesCommand).resolves({});
      docClientMock.on(PutCommand).rejects(new Error("DynamoDB put error"));

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(500);
      expect(JSON.parse(result.body)).toEqual({
        error: "Internal server error",
      });
    });

    it("should handle malformed JSON in request body", async () => {
      const event = { ...createMockEvent({}), body: "invalid json" };

      const result = await handler(event);

      expect(result.statusCode).toBe(500);
      expect(JSON.parse(result.body)).toEqual({
        error: "Internal server error",
      });
    });
  });

  describe("Edge cases", () => {
    it("should handle multiple existing agents (return first one)", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";
      const agents = [
        {
          agentId: "agent-1",
          queueUrl:
            "https://sqs.us-east-1.amazonaws.com/123456789012/queue-1.fifo",
          resourceArn,
        },
        {
          agentId: "agent-2",
          queueUrl:
            "https://sqs.us-east-1.amazonaws.com/123456789012/queue-2.fifo",
          resourceArn,
        },
      ];

      docClientMock.on(QueryCommand).resolves({
        Items: agents,
        Count: 2,
      });

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(200);
      expect(JSON.parse(result.body)).toEqual({
        agentId: "agent-1",
        queueUrl:
          "https://sqs.us-east-1.amazonaws.com/123456789012/queue-1.fifo",
        resourceArn,
      });
    });

    it("should handle null Items in query response", async () => {
      const resourceArn =
        "arn:aws:lambda:us-east-1:123456789012:function:test-function";

      docClientMock.on(QueryCommand).resolves({
        Items: undefined,
        Count: 0,
      });
      sqsMock.on(CreateQueueCommand).resolves({
        QueueUrl:
          "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue.fifo",
      });
      sqsMock.on(SetQueueAttributesCommand).resolves({});
      docClientMock.on(PutCommand).resolves({});

      const event = createMockEvent({ resourceArn });
      const result = await handler(event);

      expect(result.statusCode).toBe(200);
      expect(JSON.parse(result.body).agentId).toBe("test-uuid-1234");
    });
  });
});
