import hashlib
import json
import logging
import time
from typing import Dict, List, Tuple

import redis

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CacheService:
    def __init__(self, config: Dict[str, any]):
        self.config = config
        self.redis_client = redis.Redis(
            host=config["host"],
            port=config["port"],
            db=config.get("db", 0),
            decode_responses=True,
        )

    def _generate_key(self, company: str, title: str) -> str:
        """Generate a cache key from company and title."""
        combined = f"{company}:{title}".lower()
        return hashlib.sha256(combined.encode()).hexdigest()

    def _serialize_value(self, seniority: int) -> str:
        """Serialize the seniority value for storage."""
        return json.dumps(
            {
                "seniority": seniority,
                "last_updated": int(time.time()),
                "ttl": self.config.get("ttl", 2592000),  # Default 30 days
            }
        )

    def _deserialize_value(self, value: str) -> int:
        """Deserialize the stored value to get the seniority."""
        return json.loads(value).get("seniority")

    def get(self, company: str, title: str) -> int:
        """Get seniority level for a single (company, title) pair."""
        key = self._generate_key(company, title)
        value = self.redis_client.get(key)
        return self._deserialize_value(value)

    def set(self, company: str, title: str, seniority: int) -> None:
        """Set seniority level for a single (company, title) pair."""
        key = self._generate_key(company, title)
        value = self._serialize_value(seniority)
        self.redis_client.set(key, value, ex=self.config.get("ttl", 2592000))

    def bulk_get(self, pairs: List[Tuple[str, str]]) -> Dict[Tuple[str, str], int]:
        """Get seniority levels for multiple (company, title) pairs."""
        pipeline = self.redis_client.pipeline()
        key_to_pair = {}
        for company, title in pairs:
            key = self._generate_key(company, title)
            key_to_pair[key] = (company, title)
            pipeline.get(key)

        results = pipeline.execute()
        return {
            key_to_pair[key]: self._deserialize_value(value)
            for key, value in zip(key_to_pair.keys(), results)
            if value is not None
        }

    def bulk_set(self, pair_seniority_dict: Dict[Tuple[str, str], int]) -> None:
        """Set seniority levels for multiple (company, title) pairs."""
        pipeline = self.redis_client.pipeline()
        for (company, title), seniority in pair_seniority_dict.items():
            key = self._generate_key(company, title)
            value = self._serialize_value(seniority)
            pipeline.set(key, value, ex=self.config.get("ttl", 2592000))
        pipeline.execute()

    def delete(self, company: str, title: str) -> None:
        """Delete a single (company, title) pair from the cache."""
        key = self._generate_key(company, title)
        self.redis_client.delete(key)

    def flush_all(self) -> None:
        """Clear the entire cache. Use with caution!"""
        self.redis_client.flushall()
