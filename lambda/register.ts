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
import { APIGatewayProxyEvent, APIGatewayProxyResult } from "aws-lambda";
import { v4 as uuidv4 } from "uuid";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const sqsClient = new SQSClient({});

export const handler = async (
  event: APIGatewayProxyEvent,
): Promise<APIGatewayProxyResult> => {
  try {
    // Parse request body
    const body = JSON.parse(event.body || "{}");
    const { resourceArn } = body;

    if (!resourceArn) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "resourceArn is required" }),
      };
    }

    // Check if resourceArn exists in agents table using GSI
    const queryCommand = new QueryCommand({
      TableName: process.env.AGENTS_TABLE!,
      IndexName: "resourceArn-index",
      KeyConditionExpression: "resourceArn = :resourceArn",
      ExpressionAttributeValues: {
        ":resourceArn": resourceArn,
      },
    });

    const existingAgent = await docClient.send(queryCommand);

    if (existingAgent.Items && existingAgent.Items.length > 0) {
      // Return existing agent data
      const agent = existingAgent.Items[0];
      return {
        statusCode: 200,
        body: JSON.stringify({
          agentId: agent.agentId,
          queueUrl: agent.queueUrl,
          resourceArn: agent.resourceArn,
        }),
      };
    }

    // Generate new agentId
    const agentId = uuidv4();
    const queueName = `agents-${agentId}.fifo`;

    // Create FIFO SQS queue
    const createQueueCommand = new CreateQueueCommand({
      QueueName: queueName,
      Attributes: {
        FifoQueue: "true",
        ContentBasedDeduplication: "true",
      },
    });

    // Create FIFO SQS queue
    const queueResult = await sqsClient.send(createQueueCommand);
    const queueUrl = queueResult.QueueUrl!;

    // Grant the resourceArn permission to poll (ReceiveMessage & DeleteMessage)
    const queuePolicy = {
      Version: "2012-10-17",
      Statement: [
        {
          Sid: "AllowResourceToPollQueue",
          Effect: "Allow",
          Principal: { AWS: resourceArn },
          Action: [
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:GetQueueUrl",
          ],
          Resource: `arn:aws:sqs:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT_ID}:${queueName}`,
        },
      ],
    };
    await sqsClient.send(
      new SetQueueAttributesCommand({
        QueueUrl: queueUrl,
        Attributes: {
          Policy: JSON.stringify(queuePolicy),
        },
      }),
    );

    // Save agent to DynamoDB
    const putCommand = new PutCommand({
      TableName: process.env.AGENTS_TABLE!,
      Item: {
        resourceArn,
        agentId,
        queueUrl,
        createdAt: new Date().toISOString(),
      },
    });

    await docClient.send(putCommand);

    return {
      statusCode: 200,
      body: JSON.stringify({
        agentId,
        queueUrl,
        resourceArn,
      }),
    };
  } catch (error) {
    console.error("Error in register function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal server error" }),
    };
  }
};
