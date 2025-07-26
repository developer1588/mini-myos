import axios from "axios";

import {
  type Agent,
  getApiUrl,
  getTableNames,
  registerAgent,
  verifySubscription,
} from "./test-utils";

describe("PUT /agents/{listenerId}/subscriptions/{producerId} (integration)", () => {
  let apiUrl: string;
  let subsTableName: string;
  let listenerAgent: Agent;
  let producerAgent: Agent;

  beforeEach(async () => {
    apiUrl = await getApiUrl();
    const tables = await getTableNames();
    subsTableName = tables.subscribersTable;

    // Register two agents for testing subscriptions
    listenerAgent = await registerAgent(
      apiUrl,
      "arn:aws:test:listener-resource",
    );
    producerAgent = await registerAgent(
      apiUrl,
      "arn:aws:test:producer-resource",
    );
  });

  it("creates a new subscription with correct values", async () => {
    const response = await axios.put(
      apiUrl +
        `agents/${listenerAgent.agentId}/subscriptions/${producerAgent.agentId}`,
      {},
      { headers: { "Content-Type": "application/json" } },
    );
    expect(response.status).toBe(200);

    // Verify the subscription was created in DynamoDB with correct values
    const subscriptionExists = await verifySubscription(
      subsTableName,
      producerAgent.agentId,
      listenerAgent.agentId,
    );
    expect(subscriptionExists).toBe(true);
  });

  it("allows creating subscription with any agent IDs", async () => {
    const listenerId = "any-listener-id";
    const producerId = "any-producer-id";

    const response = await axios.put(
      apiUrl + `agents/${listenerId}/subscriptions/${producerId}`,
      {},
      { headers: { "Content-Type": "application/json" } },
    );
    expect(response.status).toBe(200);

    // Verify the subscription was created with the specified IDs
    const subscriptionExists = await verifySubscription(
      subsTableName,
      producerId,
      listenerId,
    );
    expect(subscriptionExists).toBe(true);
  });

  it("allows multiple subscriptions for same listener", async () => {
    // Register another producer
    const anotherProducer = await registerAgent(
      apiUrl,
      "arn:aws:test:another-producer",
    );

    const response = await axios.put(
      apiUrl +
        `agents/${listenerAgent.agentId}/subscriptions/${anotherProducer.agentId}`,
      {},
      { headers: { "Content-Type": "application/json" } },
    );
    expect(response.status).toBe(200);

    // Verify both subscriptions exist
    const subscription1Exists = await verifySubscription(
      subsTableName,
      producerAgent.agentId,
      listenerAgent.agentId,
    );
    const subscription2Exists = await verifySubscription(
      subsTableName,
      anotherProducer.agentId,
      listenerAgent.agentId,
    );

    expect(subscription1Exists).toBe(true);
    expect(subscription2Exists).toBe(true);
  });

  it("allows multiple listeners for same producer", async () => {
    // Register another listener
    const anotherListener = await registerAgent(
      apiUrl,
      "arn:aws:test:another-listener",
    );

    const response = await axios.put(
      apiUrl +
        `agents/${anotherListener.agentId}/subscriptions/${producerAgent.agentId}`,
      {},
      { headers: { "Content-Type": "application/json" } },
    );
    expect(response.status).toBe(200);

    // Verify both subscriptions exist for the same producer
    const subscription1Exists = await verifySubscription(
      subsTableName,
      producerAgent.agentId,
      listenerAgent.agentId,
    );
    const subscription2Exists = await verifySubscription(
      subsTableName,
      producerAgent.agentId,
      anotherListener.agentId,
    );

    expect(subscription1Exists).toBe(true);
    expect(subscription2Exists).toBe(true);
  });
});
