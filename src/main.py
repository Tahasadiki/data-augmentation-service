import logging
import sys

from batch_processor.processor import BatchProcessor
from s3_watcher.watcher import S3Watcher

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_watcher():
    watcher = S3Watcher(config_path="config/config.yaml")
    watcher.start_watching()


def run_processor():
    batch_processor = BatchProcessor(
        config_path="config/config.yaml",
    )
    batch_processor.start_processing()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ["watcher", "processor"]:
        print("Usage: python main.py [watcher|processor]")
        sys.exit(1)

    if sys.argv[1] == "watcher":
        run_watcher()
    else:
        run_processor()
