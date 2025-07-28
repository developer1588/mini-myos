import {
  CreateEventSourceMappingCommand,
  LambdaClient,
} from "@aws-sdk/client-lambda";
import { GetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";

import {
  type Agent,
  cleanDynamoDBTable,
  cleanKinesisStream,
  createSubscription,
  getApiUrl,
  getStackOutput,
  getStreamName,
  getTableNames,
  purgeQueue,
  registerAgent,
  sendEventToKinesis,
  waitForEvents,
  waitForKinesisStream,
} from "./test-utils";

describe("Agent Lambda Integration Test", () => {
  let apiUrl: string;
  let streamName: string;
  let tableNames: any;
  let subscriberAgent: Agent;
  let producerAgent: Agent;

  beforeEach(async () => {
    // Get infrastructure details
    apiUrl = await getApiUrl();
    streamName = await getStreamName();
    tableNames = await getTableNames();

    // Clean up tables and stream before each test
    await cleanDynamoDBTable(tableNames.agentsTable);
    await cleanDynamoDBTable(tableNames.subscribersTable);
    await cleanDynamoDBTable(tableNames.subEventsTable);
    await cleanKinesisStream(streamName);

    // Register subscriber agent (this will be the Lambda that processes events)
    subscriberAgent = await registerAgent(
      apiUrl,
      await getStackOutput("AgentLambdaArn"),
    );

    // Clean up the SQS queue for the subscriber agent
    await purgeQueue(subscriberAgent.queueUrl!);

    // Register producer agent
    producerAgent = await registerAgent(
      apiUrl,
      "arn:aws:test:producer-resource",
    );

    // Create subscription: subscriber listens to producer
    await createSubscription(
      apiUrl,
      subscriberAgent.agentId,
      producerAgent.agentId,
    );

    // Subscribe the agent Lambda to the SQS queue
    const sqsClient = new SQSClient({
      region: process.env.AWS_REGION,
      endpoint: process.env.LOCALSTACK_ENDPOINT,
    });
    const lambdaClient = new LambdaClient({
      region: process.env.AWS_REGION,
      endpoint: process.env.LOCALSTACK_ENDPOINT,
    });
    const { Attributes } = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: subscriberAgent.queueUrl!,
        AttributeNames: ["QueueArn"],
      }),
    );
    const queueArn = Attributes!.QueueArn!;
    await lambdaClient.send(
      new CreateEventSourceMappingCommand({
        EventSourceArn: queueArn,
        FunctionName: subscriberAgent.resourceArn,
        Enabled: true,
        BatchSize: 1,
      }),
    );
  });

  it("should process events from producer and send own event to Kinesis", async () => {
    // Send an event to Kinesis stream from the producer
    const producerEvent = {
      producerId: producerAgent.agentId,
      data: { message: "Hello from producer" },
      timestamp: new Date().toISOString(),
    };
    await sendEventToKinesis(streamName, producerEvent);

    // Check if any events were recorded in the SubEvents table
    const subEvents = await waitForEvents(tableNames.subEventsTable, 1, 20000);

    expect(subEvents.length).toBeGreaterThanOrEqual(1);

    // Verify the event structure
    const event = subEvents[0];
    expect(event.subscriberId?.S).toBe(subscriberAgent.agentId);
    expect(event.timestamp?.S).toBeDefined();
    expect(event.data?.M).toEqual({ message: { S: "Hello from producer" } });

    // Wait for the agent Lambda to process the event and send its own event
    // The Lambda should send an event with its agentId as the partition key
    // We need to wait and check for events in the stream or check SQS if the agent has a queue

    // Verify the event was sent by agent to Kinesis stream
    const kmsgs = await waitForKinesisStream(
      streamName,
      subscriberAgent.agentId,
      1,
      15000,
    );
    expect(kmsgs).toHaveLength(1);
    expect(kmsgs[0].agentId).toEqual(subscriberAgent.agentId);
    expect(kmsgs[0].data).toEqual({ message: "Hello from agent" });
  });
});
