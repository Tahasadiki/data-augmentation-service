import logging
import time
from typing import Dict, List, Tuple

import grpc

# Import the generated gRPC classes
# pylint: disable=import-error
from seniority_pb2 import SeniorityRequest, SeniorityRequestBatch
from seniority_pb2_grpc import SeniorityModelStub  # pylint: disable=import-error

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, requests_per_second: int):
        self.requests_per_second = requests_per_second
        self.last_request_time = 0

    def acquire(self, batch_size: int):
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        required_interval = batch_size / self.requests_per_second

        if time_since_last_request < required_interval:
            wait_time = required_interval - time_since_last_request
            logger.info(f"Rate limiting: waiting for {wait_time:.2f} seconds")
            time.sleep(wait_time)

        self.last_request_time = time.time()


class GrpcClient:
    def __init__(self, config: Dict[str, any]):
        self.config = config
        self.channel = grpc.insecure_channel(f"{config['host']}:{config['port']}")
        self.stub = SeniorityModelStub(self.channel)
        self.rate_limiter = RateLimiter(config.get("rate_limit", 1000))

    def infer_seniority_batch(
        self, pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], int]:
        """Infer seniority levels for a batch of (company, title) pairs."""
        batch_size = self.config.get("batch_size", 1000)
        results = {}

        for i in range(0, len(pairs), batch_size):
            batch = pairs[i : i + batch_size]

            try:
                request_batch = SeniorityRequestBatch(
                    batch=[
                        SeniorityRequest(uuid=idx, company=company, title=title)
                        for idx, (company, title) in enumerate(batch)
                    ]
                )

                response_batch = self.stub.InferSeniority(request_batch)

                for response in response_batch.batch:
                    company, title = batch[response.uuid]
                    results[(company, title)] = response.seniority

                # Apply rate limiting
                self.rate_limiter.acquire(len(batch))

            except grpc.RpcError as e:
                logger.error(f"RPC failed: {e}")

        return results

    def close(self):
        """Close the gRPC channel."""
        self.channel.close()
