import json
import logging

import boto3
import yaml
from botocore.exceptions import ClientError

from cache_service.cache import CacheService
from data_augmenter.augmenter import DataAugmenter
from grpc_client.client import GrpcClient
from s3_writer.writer import S3Writer

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchProcessor:

    def __init__(self, config_path="config/config.yaml"):
        self.config = self.load_config(config_path)
        self.s3_client = boto3.client("s3")
        self.sqs_client = boto3.client("sqs")
        self.cache_service = CacheService(self.config["redis"])
        self.grpc_client = GrpcClient(self.config["grpc"])
        self.data_augmenter = DataAugmenter()
        self.s3_writer = S3Writer(self.config["s3"]["output_bucket"])

    def load_config(self, config_path):
        with open(config_path, "r", encoding="utf-8") as config_file:
            return yaml.safe_load(config_file)

    def process_file(self, bucket, key):
        logger.info(f"Processing file: s3://{bucket}/{key}")
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")

            job_postings = [json.loads(line) for line in content.splitlines()]
            batches = self.create_batches(job_postings)

            for batch in batches:
                self.process_batch(batch)

            logger.info(f"Finished processing file: s3://{bucket}/{key}")
        except ClientError as e:
            logger.error(f"Error reading file s3://{bucket}/{key}: {str(e)}")

    def create_batches(self, job_postings):
        batch_size = self.config["batch_size"]
        return [
            job_postings[i : i + batch_size]
            for i in range(0, len(job_postings), batch_size)
        ]

    def process_batch(self, batch):
        # Deduplicate (company, title) pairs
        unique_pairs = {
            (posting["company"], posting["title"]): posting for posting in batch
        }

        # Check cache for existing seniority levels
        cache_results = self.cache_service.bulk_get(list(unique_pairs.keys()))

        # Prepare gRPC requests for cache misses
        grpc_requests = [
            (company, title)
            for (company, title) in unique_pairs.keys()
            if (company, title) not in cache_results
        ]

        # Make gRPC calls for cache misses
        if grpc_requests:
            grpc_results = self.grpc_client.infer_seniority_batch(grpc_requests)

            # Update cache with new results
            self.cache_service.bulk_set(grpc_results)

            # Merge gRPC results with cache results
            cache_results.update(grpc_results)

        # Augment job postings with seniority data
        augmented_batch = []
        for posting in batch:
            company, title = posting["company"], posting["title"]
            seniority = cache_results.get((company, title))
            if seniority is not None:
                augmented_posting = self.data_augmenter.augment(posting, seniority)
                augmented_batch.append(augmented_posting)
            else:
                logger.warning(f"No seniority data for {company} - {title}")

        # Write augmented batch to S3
        if augmented_batch:
            self.s3_writer.write_batch(augmented_batch, "job-postings-mod")

    def start_processing(self):
        logger.info("Starting batch processor...")
        while True:
            try:
                # Long polling SQS for file processing messages
                response = self.sqs_client.receive_message(
                    QueueUrl=self.config["sqs"]["queue_url"],
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                )

                if "Messages" in response:
                    for message in response["Messages"]:
                        file_info = json.loads(message["Body"])
                        self.process_file(file_info["bucket"], file_info["key"])

                        # Delete the processed message
                        self.sqs_client.delete_message(
                            QueueUrl=self.config["sqs"]["queue_url"],
                            ReceiptHandle=message["ReceiptHandle"],
                        )

            except Exception as e:
                logger.error(f"Error in batch processor: {str(e)}")
