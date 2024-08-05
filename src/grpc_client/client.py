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
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.last_update = time.time()

    def acquire(self):
        now = time.time()
        time_passed = now - self.last_update
        self.tokens = min(self.rate_limit, self.tokens + time_passed * self.rate_limit)
        self.last_update = now

        if self.tokens < 1:
            return False
        self.tokens -= 1
        return True


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
            while not self.rate_limiter.acquire():
                time.sleep(0.01)  # Wait for 10ms before trying again

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

            except grpc.RpcError as e:
                logger.error(f"RPC failed: {e}")

        return results

    def close(self):
        """Close the gRPC channel."""
        self.channel.close()
