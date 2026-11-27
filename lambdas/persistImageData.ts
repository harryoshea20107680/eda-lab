import { SQSBatchItemFailure, SQSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SQSHandler = async (event) => {
  console.log("Event ", JSON.stringify(event));

  // Record the IDs of failed SQS messages.
  const batchItemFailures: SQSBatchItemFailure[] = [];

  for (const record of event.Records) {
    try {
      // Parse SQS message.
      const recordBody = JSON.parse(record.body);

      // Parse SNS message.
      const snsMessage = JSON.parse(recordBody.Message);

      console.log("Record body ", JSON.stringify(snsMessage));

      if (snsMessage.Records) {
        for (const s3Message of snsMessage.Records) {
          const s3e = s3Message.s3;

          // Decode the image filename.
          const srcKey = decodeURIComponent(
            s3e.object.key.replace(/\+/g, " ")
          );

          // Determine file extension.
          const typeMatch = srcKey.match(/\.([^.]*)$/);

          if (!typeMatch) {
            throw new Error("Could not determine the image type.");
          }

          // Check supported image types.
          const imageType = typeMatch[1].toLowerCase();

          if (
            imageType !== "jpeg" &&
            imageType !== "jpg" &&
            imageType !== "png"
          ) {
            throw new Error(`Unsupported image type: ${imageType}`);
          }

          // Save valid images to DynamoDB.
          await ddbDocClient.send(
            new PutCommand({
              TableName: process.env.TABLE_NAME,
              Item: {
                name: srcKey,
              },
            })
          );
        }
      }
    } catch (error) {
      // Only mark this SQS message as failed.
      console.log(`Message ${record.messageId} failed: `, error);

      batchItemFailures.push({
        itemIdentifier: record.messageId,
      });
    }
  }

  // Return failed message identifiers to Lambda.
  return { batchItemFailures };
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({
    region: process.env.REGION,
  });

  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };

  const unmarshallOptions = {
    wrapNumbers: false,
  };

  const translateConfig = {
    marshallOptions,
    unmarshallOptions,
  };

  return DynamoDBDocumentClient.from(
    ddbClient,
    translateConfig
  );
}
