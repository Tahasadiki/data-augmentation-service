import logging
from typing import Any, Dict

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataAugmenter:
    def __init__(self):
        self.seniority_levels = {
            1: "Entry Level",
            2: "Junior Level",
            3: "Associate Level",
            4: "Manager Level",
            5: "Director Level",
            6: "Executive Level",
            7: "Senior Executive Level",
        }

    def augment(self, job_posting: Dict[str, Any], seniority: int) -> Dict[str, Any]:
        """
        Augment a job posting with seniority information.

        Args:
            job_posting (dict): The original job posting data.
            seniority (int): The seniority level (1-7) inferred for this job posting.

        Returns:
            dict: The augmented job posting with added seniority information.
        """
        try:
            augmented_posting = (
                job_posting.copy()
            )  # Create a copy to avoid modifying the original

            # Add numeric seniority level
            augmented_posting["seniority"] = seniority

            # Add textual seniority level
            augmented_posting["seniority_level"] = self.seniority_levels.get(
                seniority, "Unknown"
            )

            return augmented_posting
        except Exception as e:
            logger.error(f"Error augmenting job posting: {str(e)}")
            logger.error(f"Job posting: {job_posting}")
            logger.error(f"Seniority: {seniority}")
            return job_posting  # Return original posting if augmentation fails
