import * as cdk from "aws-cdk-lib";
import { Duration } from "aws-cdk-lib";
import * as apigw from "aws-cdk-lib/aws-apigateway";
import * as cw from "aws-cdk-lib/aws-cloudwatch";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kinesis from "aws-cdk-lib/aws-kinesis";
import * as lambda from "aws-cdk-lib/aws-lambda";
import {
  DynamoEventSource,
  KinesisEventSource,
} from "aws-cdk-lib/aws-lambda-event-sources";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import * as path from "path";

const app = new cdk.App();
const stack = new cdk.Stack(app, "MyOS");

// ----------------
// Streams & Tables
// ----------------
const eventsStream = new kinesis.Stream(stack, "EventsStream", {
  shardCount: 1,
});

const agentsTable = new ddb.Table(stack, "AgentsTable", {
  partitionKey: { name: "agentId", type: ddb.AttributeType.STRING },
});

agentsTable.addGlobalSecondaryIndex({
  indexName: "resourceArn-index",
  partitionKey: { name: "resourceArn", type: ddb.AttributeType.STRING },
});

const subsTable = new ddb.Table(stack, "SubscribersTable", {
  partitionKey: { name: "producerId", type: ddb.AttributeType.STRING },
  sortKey: { name: "subscriberId", type: ddb.AttributeType.STRING },
});

const subEventsTable = new ddb.Table(stack, "SubEventsTable", {
  partitionKey: { name: "subscriberId", type: ddb.AttributeType.STRING },
  sortKey: { name: "timestamp", type: ddb.AttributeType.STRING },
  stream: ddb.StreamViewType.NEW_IMAGE,
});

// ----------------
// Lambda Functions
// ----------------
const registerFn = new NodejsFunction(stack, "RegisterFn", {
  runtime: lambda.Runtime.NODEJS_22_X,
  entry: path.join(process.cwd(), "lambda/register.ts"),
  tracing: lambda.Tracing.ACTIVE,
  environment: {
    AGENTS_TABLE: agentsTable.tableName,
    AWS_ACCOUNT_ID: stack.account,
  },
});

const aggregatorFn = new NodejsFunction(stack, "AggregatorFn", {
  runtime: lambda.Runtime.NODEJS_22_X,
  entry: path.join(process.cwd(), "lambda/aggregator.ts"),
  tracing: lambda.Tracing.ACTIVE,
  environment: {
    SUBSCRIBERS_TABLE: subsTable.tableName,
    SUB_EVENTS_TABLE: subEventsTable.tableName,
  },
});

const publisherFn = new NodejsFunction(stack, "PublisherFn", {
  runtime: lambda.Runtime.NODEJS_22_X,
  entry: path.join(process.cwd(), "lambda/publisher.ts"),
  tracing: lambda.Tracing.ACTIVE,
  environment: {
    SUB_EVENTS_TABLE: subEventsTable.tableName,
    AGENTS_TABLE: agentsTable.tableName,
  },
});

const agentFn = new NodejsFunction(stack, "AgentFn", {
  runtime: lambda.Runtime.NODEJS_22_X,
  entry: path.join(process.cwd(), "lambda/agent.ts"),
  tracing: lambda.Tracing.ACTIVE,
  environment: {
    EVENTS_STREAM: eventsStream.streamName,
    AGENTS_TABLE: agentsTable.tableName,
  },
});

// --------------------
// IAM Roles & Policies
// --------------------

// API Gateway role for direct DynamoDB/Kinesis integrations
const apiRole = new iam.Role(stack, "ApiGatewayRole", {
  assumedBy: new iam.ServicePrincipal("apigateway.amazonaws.com"),
});

// Permissions for Lambdas
registerFn.addToRolePolicy(
  new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ["sqs:CreateQueue", "sqs:SetQueueAttributes"],
    resources: ["arn:aws:sqs:*:*:agents-*"],
  }),
);

publisherFn.addToRolePolicy(
  new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ["sqs:SendMessage"],
    resources: ["arn:aws:sqs:*:*:agents-*"],
  }),
);

// Grant table access to API Gateway role
agentsTable.grantWriteData(apiRole);
subsTable.grantWriteData(apiRole);

// Grant table/stream access to Lambdas
agentsTable.grantReadWriteData(registerFn);
subsTable.grantReadData(aggregatorFn);
subEventsTable.grantWriteData(aggregatorFn);
subEventsTable.grantReadData(publisherFn);
agentsTable.grantReadData(publisherFn);
agentsTable.grantReadData(agentFn);

// Event Source Mappings
aggregatorFn.addEventSource(
  new KinesisEventSource(eventsStream, {
    startingPosition: lambda.StartingPosition.TRIM_HORIZON,
  }),
);

publisherFn.addEventSource(
  new DynamoEventSource(subEventsTable, {
    startingPosition: lambda.StartingPosition.TRIM_HORIZON,
  }),
);

// --------------------------
// API Gateway & Integrations
// --------------------------
const api = new apigw.RestApi(stack, "Api");

// POST /agents → registerFn
api.root
  .addResource("agents")
  .addMethod("POST", new apigw.LambdaIntegration(registerFn), {
    methodResponses: [{ statusCode: "200" }],
  });

// PUT /agents/{listenerId}/subscriptions/{producerId} → DynamoDB PutItem
const putSubscription = new apigw.AwsIntegration({
  service: "dynamodb",
  action: "PutItem",
  integrationHttpMethod: "POST",
  options: {
    credentialsRole: apiRole,
    passthroughBehavior: apigw.PassthroughBehavior.NEVER,
    requestTemplates: {
      "application/json": JSON.stringify({
        TableName: subsTable.tableName,
        Item: {
          producerId: { S: "$input.params('producerId')" },
          subscriberId: { S: "$input.params('listenerId')" },
        },
      }),
    },
    integrationResponses: [
      { statusCode: "200", selectionPattern: ".*" },
      { statusCode: "400", selectionPattern: "ValidationException|BadRequest" },
      { statusCode: "500", selectionPattern: ".*" },
    ],
  },
});

api.root
  .getResource("agents")!
  .addResource("{listenerId}")
  .addResource("subscriptions")
  .addResource("{producerId}")
  .addMethod("PUT", putSubscription, {
    methodResponses: [
      { statusCode: "200" },
      { statusCode: "400" },
      { statusCode: "500" },
    ],
  });

// POST /events → Kinesis PutRecord
eventsStream.grantWrite(apiRole);

const putEventRecord = new apigw.AwsIntegration({
  service: "kinesis",
  action: "PutRecord",
  integrationHttpMethod: "POST",
  options: {
    credentialsRole: apiRole,
    requestTemplates: {
      "application/json": `{
        "StreamName":"${eventsStream.streamName}",
        "Data":"$util.base64Encode($input.body)",
        "PartitionKey":"$input.path('$.partitionKey')"
      }`,
    },
    integrationResponses: [{ statusCode: "200" }],
  },
});

api.root.addResource("events").addMethod("POST", putEventRecord, {
  methodResponses: [{ statusCode: "200" }],
});

// ---------------------------
// Outputs
// ---------------------------
new cdk.CfnOutput(stack, "ApiUrl", {
  value: api.url,
  description: "Base URL for the REST API",
});

new cdk.CfnOutput(stack, "StreamName", {
  value: eventsStream.streamName,
  description: "Name of the Kinesis events stream",
});

new cdk.CfnOutput(stack, "AgentLambdaArn", {
  value: agentFn.functionArn,
  description: "ARN of the Agent Lambda function",
});

// ---------------------------
// CloudWatch Alarms
// ---------------------------

new cw.Alarm(stack, "AggregatorThrottlesAlarm", {
  metric: aggregatorFn.metricThrottles({ period: Duration.minutes(1) }),
  threshold: 1,
  evaluationPeriods: 1,
  alarmName: "AggregatorFn-Throttles",
  alarmDescription: "Alarm if AggregatorFn is throttled in a 1‑minute window",
});

new cw.Alarm(stack, "KinesisIteratorAgeAlarm", {
  metric: eventsStream.metricGetRecordsIteratorAgeMilliseconds({
    statistic: "Maximum",
    period: Duration.minutes(1),
  }),
  threshold: 60000,
  evaluationPeriods: 1,
  alarmName: "EventsStream-IteratorAge",
  alarmDescription:
    "Alarm if the oldest un-processed record in the Kinesis stream is over 1 minute old",
});

app.synth();
