import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import {
  DynamoDBDocumentClient,
  GetCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const sqsClient = new SQSClient({});

interface SubEvent {
  subscriberId: string;
  timestamp: string;
  [key: string]: any;
}

interface Agent {
  agentId: string;
  queueUrl: string;
  [key: string]: any;
}

export const handler = async (
  event: DynamoDBStreamEvent,
): Promise<{ statusCode: number }> => {
  // Group stream events by subscriberId
  const eventsBySubscriber: Record<string, DynamoDBRecord[]> = {};

  for (const record of event.Records) {
    if (record.eventName === "INSERT" && record.dynamodb?.NewImage) {
      const subscriberId = record.dynamodb.NewImage.subscriberId?.S;
      if (subscriberId) {
        if (!eventsBySubscriber[subscriberId]) {
          eventsBySubscriber[subscriberId] = [];
        }
        eventsBySubscriber[subscriberId].push(record);
      }
    }
  }

  // Process each subscriber's events
  for (const [subscriberId, records] of Object.entries(eventsBySubscriber)) {
    // Get the number of events in the stream for this subscriber
    const eventCount = records.length;

    // Query SubEventsTable to get the latest events for this subscriber
    const queryResponse = await docClient.send(
      new QueryCommand({
        TableName: process.env.SUB_EVENTS_TABLE!,
        KeyConditionExpression: "subscriberId = :subscriberId",
        ExpressionAttributeValues: {
          ":subscriberId": subscriberId,
        },
        ScanIndexForward: false, // Sort by timestamp descending
        Limit: eventCount,
      }),
    );

    const sortedEvents = queryResponse.Items as SubEvent[];

    if (sortedEvents.length === 0) {
      console.log(`No events found for subscriber ${subscriberId}`);
      continue;
    }

    // Get queue ARN from AgentsTable
    const agentResponse = await docClient.send(
      new GetCommand({
        TableName: process.env.AGENTS_TABLE!,
        Key: {
          agentId: subscriberId,
        },
      }),
    );

    const agent = agentResponse.Item as Agent;
    if (!agent?.queueUrl) {
      console.error(`No queue ARN found for agent ${subscriberId}`);
      continue;
    }

    // Send sorted batch to SQS queue
    const message = {
      subscriberId,
      events: sortedEvents,
      timestamp: new Date().toISOString(),
    };

    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: agent.queueUrl,
        MessageBody: JSON.stringify(message),
        MessageGroupId: subscriberId,
      }),
    );
  }

  return { statusCode: 200 };
};
