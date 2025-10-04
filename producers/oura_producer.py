"""
oura_producer.py

Stream JSON data to a Kafka topic.

Example serialized to Kafka message
{
    "score": 81, 
    "active_calories": 356, 
    "average_met_minutes": 1.53125, 
    "contributors": {
        "meet_daily_targets": 60, 
        "move_every_hour": 95, 
        "recovery_time": 98, 
        "stay_active": 56, 
        "training_frequency": 96, 
        "training_volume": 97}, 
    "equivalent_walking_distance": 4659, 
    "high_activity_met_minutes": 0, 
    "high_activity_time": 0, 
    "inactivity_alerts": 1, 
    "low_activity_met_minutes": 198, 
    "low_activity_time": 20340, 
    "medium_activity_met_minutes": 28, 
    "medium_activity_time": 480, 
    "meters_to_target": 3200, 
    "non_wear_time": 0, 
    "resting_time": 10020, 
    "sedentary_met_minutes": 14, 
    "sedentary_time": 38820, 
    "steps": 6072, 
    "target_calories": 550, 
    "target_meters": 10000, 
    "total_calories": 2978, 
    "day": "2022-01-28", 
    "timestamp": "2022-01-28T04:00:00-06:00"
}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import pathlib  # work with file paths
import json  # work with JSON data

# Import external packages
from dotenv import load_dotenv  # type: ignore

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger # type: ignore

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("OURA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("OURA_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("oura.jsonl")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################


def generate_messages(file_path: pathlib.Path):
    """
    Read from a JSON file and yield them one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the JSON file.

    Yields:
        dict: A dictionary containing the JSON data.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {DATA_FILE}")
            with open(DATA_FILE, "r") as json_file:
                logger.info(f"Reading data from file: {DATA_FILE}")

                # Read and parse each line as a separate JSON object (for .jsonl files)
                line_num = 0
                for line in json_file:
                    line_num += 1
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        oura_entry = json.loads(line)
                        logger.debug(f"Generated JSON from line {line_num}: {oura_entry}")
                        yield oura_entry
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON on line {line_num} in file {DATA_FILE}: {e}\nOffending line: {line}")
                        continue
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {file_path}. Error: {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated JSON messages to the Kafka topic.
    """

    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in generate_messages(DATA_FILE):
            # Send message directly as a dictionary (producer handles serialization)
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
