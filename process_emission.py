import json
import logging
import sys
import greengrasssdk
import time

# --- Configuration and State ---
# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Greengrass V1 SDK Client (used for IPC PubSub on the core)
# NOTE: This client is designed for publishing to local topics 
# which the MQTT Bridge then relays to the client devices.
client = greengrasssdk.client("iot-data")

# Global state to track the maximum CO2 value seen for each vehicle ID.
# This persists across multiple Lambda invocations as long as the Lambda container stays active.
MAX_CO2_STATE = {}

# --- Topic Configuration ---
# Topic where the clients subscribe to receive the calculated MAX CO2 value.
# The {vehicle_id} ensures only the correct device receives the result.
RESULTS_TOPIC_FORMAT = "vehicle/results/{}/max_co2"

# --- Handler Function ---
def lambda_handler(event, context):
    logger.info("Lambda Handler Invoked. Processing {} records.".format(len(event)))
    
    # Check if the event is a list (typical for batched messages)
    if not isinstance(event, list):
        event = [event] # Handle single message case

    # Use a set to track which vehicles were updated in this batch
    updated_vehicles = set()

    for record in event:
        try:
            # The client simulator publishes a JSON payload, which the Lambda runtime
            # might have already deserialized into a dictionary.
            data = record # Assuming the incoming message is the dictionary payload

            vehicle_id = str(data.get('vehicle_id'))
            co2_val = float(data.get('CO2')) # Assuming the CO2 column header is 'CO2' in your data
            
            if not vehicle_id:
                logger.warn("Skipping record: vehicle_id not found.")
                continue

            # 1. Get current max, defaulting to the received value if the vehicle is new
            current_max = MAX_CO2_STATE.get(vehicle_id, co2_val)
            
            # 2. Check if the new reading is the maximum
            if co2_val > current_max:
                MAX_CO2_STATE[vehicle_id] = co2_val
                updated_vehicles.add(vehicle_id)
                logger.info("New max CO2 recorded for Vehicle {}: {}".format(vehicle_id, co2_val))
            
        except Exception as e:
            logger.error("Error processing record: {} - {}".format(record, e))
            
    # 3. Publish the result back to the device (Part 2.2)
    # We only publish the final maximum for vehicles that had an updated max in this batch.
    for vehicle_id in updated_vehicles:
        publish_max_co2(vehicle_id, MAX_CO2_STATE[vehicle_id])
    
    logger.info("Processing complete. Current max state: {}".format(json.dumps(MAX_CO2_STATE)))
    return

def publish_max_co2(vehicle_id, max_co2_value):
    """Publishes the max CO2 result to the unique topic for the specific vehicle."""
    
    target_topic = RESULTS_TOPIC_FORMAT.format(vehicle_id)
    payload_data = {
        "vehicle_id": vehicle_id,
        "max_CO2": max_co2_value,
        "timestamp": int(time.time())
    }
    payload_json = json.dumps(payload_data)
    
    try:
        client.publish(
            topic=target_topic,
            queueFullPolicy="AllOrException",
            payload=payload_json
        )
        logger.info("Published result for Vehicle {} to {}: {}".format(vehicle_id, target_topic, payload_json))
    except Exception as e:
        logger.error("Failed to publish result to {}: {}".format(target_topic, e))