import json
import logging

import boto3
import yaml
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Watcher:
    def __init__(self, config_path="config/config.yaml"):
        self.config = self.load_config(config_path)
        self.s3_client = boto3.client("s3")
        self.sqs_client = boto3.client("sqs")

    def load_config(self, config_path):
        with open(config_path, "r", encoding="utf-8") as config_file:
            return yaml.safe_load(config_file)

    def process_s3_event(self, event):
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            if not key.endswith(".jsonl"):
                logger.info(f"Skipping non-JSONL file: {key}")
                continue

            logger.info(f"Processing new file: s3://{bucket}/{key}")

            try:
                # Prepare message for batch processor
                message = {"bucket": bucket, "key": key}

                # Send message to SQS queue
                self.sqs_client.send_message(
                    QueueUrl=self.config["sqs"]["queue_url"],
                    MessageBody=json.dumps(message),
                )

                logger.info(f"Sent message to SQS for file: {key}")

            except ClientError as e:
                logger.error(f"Error processing file {key}: {str(e)}")

    def start_watching(self):
        logger.info("Starting S3 watcher...")
        while True:
            try:
                # Long polling SQS for S3 events
                response = self.sqs_client.receive_message(
                    QueueUrl=self.config["sqs"]["s3_events_queue_url"],
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                )

                if "Messages" in response:
                    for message in response["Messages"]:
                        event = json.loads(message["Body"])
                        self.process_s3_event(event)

                        # Delete the processed message
                        self.sqs_client.delete_message(
                            QueueUrl=self.config["sqs"]["queue_url"],
                            ReceiptHandle=message["ReceiptHandle"],
                        )

            except Exception as e:  # pylint: disable=broad-except
                logger.error(f"Error in S3 watcher: {str(e)}")
