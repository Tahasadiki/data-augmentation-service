import json
import logging
import uuid
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Writer:
    def __init__(self, bucket_name: str, config: Dict[str, Any] = None):
        self.bucket_name = bucket_name
        self.config = config or {}
        self.s3_client = boto3.client("s3")
        self.multipart_threshold = self.config.get(
            "multipart_threshold", 8 * 1024 * 1024
        )  # 8 MB

    def write_single(self, job_posting: Dict[str, Any], key: str) -> bool:
        """
        Write a single job posting to S3.

        Args:
            job_posting (dict): The job posting to write.
            key (str): The S3 object key.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            json_data = json.dumps(job_posting)
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=json_data)
            logger.info(
                f"Successfully wrote job posting to s3://{self.bucket_name}/{key}"
            )
            return True
        except ClientError as e:
            logger.error(f"Error writing job posting to S3: {str(e)}")
            return False

    def write_batch(self, job_postings: List[Dict[str, Any]], key_prefix: str) -> bool:
        """
        Write a batch of job postings to S3.

        Args:
            job_postings (list): List of job postings to write.
            key_prefix (str): Prefix for the S3 object key.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Convert job postings to JSONL format
            jsonl_data = "\n".join(json.dumps(posting) for posting in job_postings)

            # Generate a unique key for this batch
            unique_id = str(uuid.uuid4())
            key = f"{key_prefix}/{unique_id}.jsonl"

            # Check if we need to use multipart upload
            if len(jsonl_data.encode("utf-8")) > self.multipart_threshold:
                self._multipart_upload(jsonl_data, key)
            else:
                self.s3_client.put_object(
                    Bucket=self.bucket_name, Key=key, Body=jsonl_data
                )

            logger.info(
                f"Successfully wrote batch of {len(job_postings)} "
                f"job postings to s3://{self.bucket_name}/{key}"
            )
            return True
        except ClientError as e:
            logger.error(f"Error writing batch to S3: {str(e)}")
            return False

    def _multipart_upload(self, data: str, key: str) -> None:
        """
        Perform a multipart upload to S3.

        Args:
            data (str): The data to upload.
            key (str): The S3 object key.
        """
        mpu = self.s3_client.create_multipart_upload(Bucket=self.bucket_name, Key=key)
        parts = []

        # Split the data into chunks
        chunk_size = 8 * 1024 * 1024  # 8 MB chunks
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

        # Upload each chunk
        for i, chunk in enumerate(chunks, 1):
            part = self.s3_client.upload_part(
                Bucket=self.bucket_name,
                Key=key,
                PartNumber=i,
                UploadId=mpu["UploadId"],
                Body=chunk.encode("utf-8"),
            )
            parts.append({"PartNumber": i, "ETag": part["ETag"]})

        # Complete the multipart upload
        self.s3_client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=key,
            UploadId=mpu["UploadId"],
            MultipartUpload={"Parts": parts},
        )
