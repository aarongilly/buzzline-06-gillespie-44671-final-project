"""
oura_consumer.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

Example JSON message (after deserialization) to be analyzed:
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
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv # type: ignore

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger # type: ignore

#####################################
# Load Environment Variables
#####################################

# load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("OURA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store author counts
author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()
ax2 = ax.twinx()  # Create the secondary y-axis once

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################

def update_chart():
    """Update the live chart with the latest calories burned and days."""
    # Clear the previous chart
    ax.clear()
    ax2.clear()

    # Use the global lists for plotting
    global calories_list, days_list, activity_list

    # Create a bar chart using the bar() method.
    ax.bar(days_list, calories_list, color="skyblue", label="Calories Burned")

    # Plot the activity_list (line chart) on the secondary y-axis
    ax2.plot(days_list, activity_list, color="orange", marker="o", label="High Activity Time")
    ax2.set_ylabel("High Activity Time (mins)", color="orange")
    ax2.tick_params(axis='y', labelcolor="orange")

    # Add legends for both axes
    ax.legend(loc="upper left")
    ax2.legend(loc="upper right")

    # Use the built-in axes methods to set the labels and title
    ax.set_xlabel("Days")
    ax.set_ylabel("Calories Burned (kcal)")
    ax.set_title("Calories Burned by Day with High Activity Time")

    # Use the set_xticklabels() method to rotate the x-axis labels
    ax.set_xticklabels(days_list, rotation=45, ha="right")
    ax2.yaxis.set_label_position("right")  # Move label to the right
    ax2.yaxis.set_ticks_position("right")  # Ensure ticks are also on the right


    # Use the tight_layout() method to automatically adjust the padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow some time for the chart to render
    plt.pause(0.1)


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        # logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        # logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the "total_calories" field from the Python dictionary
            total_calories = message_dict.get("total_calories", None)
            # Extract the "day" field from the Python dictionary
            day = message_dict.get("day", "unknown_day")
            # Extract the "high_activity_time" field from the Python dictionary
            high_activity = message_dict.get("high_activity_time", None)
            
            logger.info(f"Entry day: {day}")
            logger.info(f"Entry Total Calories: {total_calories}")
            logger.info(f"Entry High Activity: {high_activity}")

            # Initialize lists to store scores and days if they don't exist
            if "calories_list" not in globals():
                global calories_list
                calories_list = []
            if "days_list" not in globals():
                global days_list
                days_list = []
            if "activity_list" not in globals():
                global activity_list
                activity_list = []

            # Append the extracted score and day to their respective lists
            calories_list.append(total_calories)
            days_list.append(day)
            activity_list.append(high_activity)

            # Update the chart
            update_chart()

            # Log the updated chart
            # logger.info(f"Chart updated successfully for message: {message}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    load_dotenv()

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # message is a complex object with metadata and value
            # Use the value attribute to extract the message as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
