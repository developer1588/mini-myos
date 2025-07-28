import { ScanCommand } from "@aws-sdk/client-dynamodb";

import {
  type Agent,
  cleanDynamoDBTable,
  cleanKinesisStream,
  createSubscription,
  ddb,
  getApiUrl,
  getStreamName,
  getTableNames,
  registerAgent,
  sendEventToKinesis,
  waitForEvents,
} from "./test-utils";

describe("Aggregator Function (integration)", () => {
  let apiUrl: string;
  let streamName: string;
  let subscribersTable: string;
  let subEventsTable: string;
  let producerAgent: Agent;
  let listenerAgent1: Agent;
  let listenerAgent2: Agent;
  let createdAgents: string[] = [];

  beforeEach(async () => {
    apiUrl = await getApiUrl();
    streamName = await getStreamName();
    const tables = await getTableNames();
    subscribersTable = tables.subscribersTable;
    subEventsTable = tables.subEventsTable;

    // Clean tables before starting tests
    await cleanDynamoDBTable(subscribersTable);
    await cleanDynamoDBTable(subEventsTable);
    await cleanKinesisStream(streamName);

    // Register agents
    producerAgent = await registerAgent(
      apiUrl,
      "arn:aws:test:producer-resource",
    );
    listenerAgent1 = await registerAgent(
      apiUrl,
      "arn:aws:test:listener1-resource",
    );
    listenerAgent2 = await registerAgent(
      apiUrl,
      "arn:aws:test:listener2-resource",
    );

    createdAgents.push(
      producerAgent.agentId,
      listenerAgent1.agentId,
      listenerAgent2.agentId,
    );

    // Create subscriptions
    await createSubscription(
      apiUrl,
      listenerAgent1.agentId,
      producerAgent.agentId,
    );
    await createSubscription(
      apiUrl,
      listenerAgent2.agentId,
      producerAgent.agentId,
    );
    await cleanDynamoDBTable(subEventsTable);
  });

  it("processes events from Kinesis and saves to subEventsTable", async () => {
    const testEvent = {
      producerId: producerAgent.agentId,
      data: { message: "Hello from producer" },
      timestamp: new Date().toISOString(),
    };

    // Send event to Kinesis
    await sendEventToKinesis(streamName, testEvent);

    // Wait for aggregator to process the event and save to subEventsTable
    // We expect 2 events (one for each subscriber)
    const events = await waitForEvents(subEventsTable, 2);

    expect(events).toHaveLength(2);

    // Verify events contain expected subscriber IDs
    const subscriberIds = events.map((item) => item.subscriberId?.S).sort();
    expect(subscriberIds).toEqual(
      [listenerAgent1.agentId, listenerAgent2.agentId].sort(),
    );

    // Verify event data is preserved
    events.forEach((item) => {
      expect(item.timestamp?.S).toBeDefined();
      expect(item.data?.M).toEqual({ message: { S: "Hello from producer" } });
    });
  });

  it("handles multiple events for the same producer", async () => {
    const event1 = {
      producerId: producerAgent.agentId,
      data: { message: "First event" },
      timestamp: new Date().toISOString(),
    };

    const event2 = {
      producerId: producerAgent.agentId,
      data: { message: "Second event" },
      timestamp: new Date(Date.now() + 1000).toISOString(),
    };

    // Send both events
    await sendEventToKinesis(streamName, event1);
    await sendEventToKinesis(streamName, event2);

    // Wait for all events to be processed (2 events × 2 subscribers = 4 events)
    const events = await waitForEvents(subEventsTable, 4);

    // Should have 4 events total
    expect(events.length).toBe(4);
  });

  it("ignores events for producers with no subscribers", async () => {
    // Register a producer with no subscribers
    const loneProducer = await registerAgent(
      apiUrl,
      "arn:aws:test:lone-producer",
    );
    createdAgents.push(loneProducer.agentId);

    const loneEvent = {
      producerId: loneProducer.agentId,
      data: { message: "Nobody listening" },
      timestamp: new Date().toISOString(),
    };

    // Send event for lone producer
    await sendEventToKinesis(streamName, loneEvent);

    // Wait a bit to ensure processing would have happened
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Check that no events were added
    const afterResult = await ddb.send(
      new ScanCommand({ TableName: subEventsTable }),
    );
    expect(afterResult.Items).toHaveLength(0);
  });
});
