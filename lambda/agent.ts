import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import { KinesisClient, PutRecordCommand } from "@aws-sdk/client-kinesis";
import { Context, SQSEvent } from "aws-lambda";

const kinesis = new KinesisClient({});
const dynamodb = new DynamoDBClient({});

export const handler = async (
  event: SQSEvent,
  context: Context,
): Promise<void> => {
  // Get the Lambda's ARN from context
  const lambdaArn = context.invokedFunctionArn;

  // Query agentsTable to find agentId by resourceArn
  const queryCommand = new QueryCommand({
    TableName: process.env.AGENTS_TABLE!,
    IndexName: "resourceArn-index",
    KeyConditionExpression: "resourceArn = :arn",
    ExpressionAttributeValues: {
      ":arn": { S: lambdaArn },
    },
  });

  const queryResult = await dynamodb.send(queryCommand);

  if (!queryResult.Items || queryResult.Items.length === 0) {
    throw new Error(`No agent found for Lambda ARN: ${lambdaArn}`);
  }

  const agentId = queryResult.Items[0].agentId?.S;
  if (!agentId) {
    throw new Error(`Agent ID not found in query result`);
  }

  console.log(`Agent ${agentId} processing ${event.Records.length} messages`);

  for (const record of event.Records) {
    console.log(`Processing message ${record.messageId} from queue`);
  }

  // Send event to Kinesis stream
  const eventData = {
    agentId: agentId,
    timestamp: new Date().toISOString(),
    data: { message: "Hello from agent" },
  };

  const command = new PutRecordCommand({
    StreamName: process.env.EVENTS_STREAM!,
    Data: Buffer.from(JSON.stringify(eventData)),
    PartitionKey: agentId,
  });

  await kinesis.send(command);
  console.log(`Event sent to Kinesis stream: ${process.env.EVENTS_STREAM!}`);
};
