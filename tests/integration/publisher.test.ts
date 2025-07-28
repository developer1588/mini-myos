import {
  addSubEvent,
  type Agent,
  cleanDynamoDBTable,
  getApiUrl,
  getTableNames,
  registerAgent,
  waitForSQSMessages,
} from "./test-utils";

describe("Publisher Function (integration)", () => {
  let apiUrl: string;
  let subscribersTable: string;
  let subEventsTable: string;
  let agentsTable: string;
  let listenerAgent: Agent;
  let testQueueUrl: string;

  beforeEach(async () => {
    apiUrl = await getApiUrl();
    const tables = await getTableNames();
    subscribersTable = tables.subscribersTable;
    subEventsTable = tables.subEventsTable;
    agentsTable = tables.agentsTable;

    // Clean tables before starting tests
    await cleanDynamoDBTable(subscribersTable);
    await cleanDynamoDBTable(subEventsTable);
    await cleanDynamoDBTable(agentsTable);

    // Register a listener agent
    listenerAgent = await registerAgent(
      apiUrl,
      "arn:aws:test:listener-resource",
    );
    testQueueUrl = listenerAgent.queueUrl!;
  });

  it("publishes events to SQS when SubEventsTable receives new items", async () => {
    const testData = {
      message: "Hello from publisher test",
      eventType: "test-event",
    };

    const timestamp = new Date().toISOString();

    // Add event to SubEventsTable (simulating aggregator output)
    await addSubEvent(
      subEventsTable,
      listenerAgent.agentId,
      timestamp,
      testData,
    );

    // The publisher lambda should be triggered by DynamoDB stream
    // Wait for message to appear in SQS queue
    const messages = await waitForSQSMessages(testQueueUrl, 1);

    expect(messages).toHaveLength(1);

    const message = messages[0];
    expect(message.subscriberId).toBe(listenerAgent.agentId);
    expect(message.events).toHaveLength(1);
    expect(message.events[0].subscriberId).toBe(listenerAgent.agentId);
    expect(message.events[0].timestamp).toBe(timestamp);
    expect(message.events[0].data.message).toBe(testData.message);
    expect(message.events[0].data.eventType).toBe(testData.eventType);
    expect(message.timestamp).toBeDefined();
  });

  it("batches multiple events for the same subscriber", async () => {
    const event1Data = {
      message: "First event",
      eventType: "event-1",
    };

    const event2Data = {
      message: "Second event",
      eventType: "event-2",
    };

    const timestamp1 = new Date().toISOString();
    const timestamp2 = new Date(Date.now() + 1000).toISOString();

    // Add multiple events to SubEventsTable
    await addSubEvent(
      subEventsTable,
      listenerAgent.agentId,
      timestamp1,
      event1Data,
    );
    await addSubEvent(
      subEventsTable,
      listenerAgent.agentId,
      timestamp2,
      event2Data,
    );

    // Wait for message to appear in SQS queue
    const messages = await waitForSQSMessages(testQueueUrl, 1);

    expect(messages).toHaveLength(1);

    const message = messages[0];
    expect(message.subscriberId).toBe(listenerAgent.agentId);
    expect(message.events).toHaveLength(2);

    // Events should be sorted by timestamp descending (newest first)
    expect(message.events[0].timestamp).toBe(timestamp2);
    expect(message.events[0].data.message).toBe(event2Data.message);
    expect(message.events[1].timestamp).toBe(timestamp1);
    expect(message.events[1].data.message).toBe(event1Data.message);
  });

  it("handles subscriber without queue URL gracefully", async () => {
    // Register another agent without setting up queue
    const anotherAgent = await registerAgent(
      apiUrl,
      "arn:aws:test:no-queue-agent",
    );

    const testData = {
      message: "Event for agent without queue",
      eventType: "test-event",
    };

    // Add event for agent without queue
    await addSubEvent(
      subEventsTable,
      anotherAgent.agentId,
      new Date().toISOString(),
      testData,
    );

    // Original test queue should remain empty
    const messages = await waitForSQSMessages(testQueueUrl, 1, 1000);
    expect(messages).toHaveLength(0);
  });
});
