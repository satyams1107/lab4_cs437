import json
import sys
import time
import logging
import boto3
import os
from awsgreengrasspubsubsdk.pubsub_client import AwsGreengrassPubSubSdkClient
from awsgreengrasspubsubsdk.message_formatter import PubSubMessageFormatter

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

print("EmissionAnalyzer starting...", flush=True)

config = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}

BASE_TOPIC = config.get("base-pubsub-topic", "com.iotea.EmissionAnalyzer")
MQTT_SUB_TOPICS = config.get("mqtt-subscribe-topics", [])

log.info(f"Base PubSub topic: {BASE_TOPIC}")
log.info(f"MQTT subscription topics: {MQTT_SUB_TOPICS}")

# Track maximum CO₂ values
MAX_CO2_STATE = {}
FIREHOSE_CLIENT = None
FIREHOSE_STREAM_NAME = "lab4"

def initialize_firehose_client():
    """Initializes the Boto3 Firehose client using Greengrass environment variables."""
    global FIREHOSE_CLIENT
    
    # Greengrass provides the region via the environment variable AWS_REGION
    aws_region = os.environ.get("AWS_REGION", "us-east-2")
    
    try:
        FIREHOSE_CLIENT = boto3.client('firehose', region_name=aws_region)
        log.info(f"Firehose client initialized for region {aws_region}. Stream Name: {FIREHOSE_STREAM_NAME}")
    except Exception as e:
        log.error(f"Failed to initialize Boto3 Firehose client: {e}")

# ==========================================================
# HANDLER CLASS
# ==========================================================
class EmissionHandler:
    def __init__(self, client):
        self.client = client
        self.formatter = PubSubMessageFormatter()

    # The SDK invokes this callback
    def on_message(self, protocol, topic, message_id, status, route, message):
        try:
            # --- START LOGGING ---
            log.info("--- START: Processing Incoming Message ---")
            log.info(f"PROTOCOL: {protocol}, TOPIC: {topic}, MESSAGE_ID: {message_id}")

            if not isinstance(message, dict):
                log.error(f"FATAL ERROR: Payload is not a dictionary, type is {type(message)}.")
                raise ValueError(f"Payload is not a dictionary: {message}")
                    
            payload = message

            log.info(f"Received message payload: {payload}")

            # Extract values
            vehicle_id = str(payload.get("vehicle_id", "unknown"))
            # Safely attempt to convert CO2, default to 0.0 if missing or conversion fails
            try:
                co2_val = float(payload.get("vehicle_CO2", 0))
            except (TypeError, ValueError):
                co2_val = 0.0
                log.error(f"Could not convert 'vehicle_CO2' value ({payload.get('vehicle_CO2')}) to float. Using 0.0.")
            
            log.info(f"Extracted values: Vehicle ID='{vehicle_id}', CO₂ Value={co2_val}")

            if vehicle_id == "unknown":
                log.error("Missing vehicle_id. Skipping processing flow.")
                return

            # --- COMPARISON LOGIC ---
            prev = MAX_CO2_STATE.get(vehicle_id, -1.0)
            
            # Log the comparison state before the if statement
            log.info(f"COMPARE: Current Max State for {vehicle_id} is {prev}. Incoming CO₂ is {co2_val}.")
            
            if co2_val > prev:
                # --- PUBLISH BRANCH START ---
                log.info(f"CONDITION MET: New max CO₂ found! {co2_val} > {prev}")
                
                MAX_CO2_STATE[vehicle_id] = co2_val
                log.info(f"STATE UPDATED: {vehicle_id} max set to {co2_val}")

                publish_topic = f"vehicle/results/{vehicle_id}/max_co2"

                result = {
                    "vehicle_id": vehicle_id,
                    "max_CO2": co2_val,
                    "timestamp": int(time.time())
                }

                log.info(f"Outgoing payload prepared: {result}")

                if FIREHOSE_CLIENT:
                    try:
                        # Firehose requires the data to be a string and encoded as bytes, often with a newline delimiter
                        record_data = json.dumps(result) + '\n' 
                        FIREHOSE_CLIENT.put_record(
                            DeliveryStreamName=FIREHOSE_STREAM_NAME,
                            Record={'Data': record_data.encode('utf-8')}
                        )
                        log.info(f"Successfully sent max CO₂ data to Firehose stream {FIREHOSE_STREAM_NAME}.")
                    except Exception as fe:
                        log.error(f"Firehose publish failed (Check IAM/network): {fe}")

                # Wrap outgoing message per SDK spec
                formatted = self.formatter.get_message(
                    message_id=message_id,
                    route="EmissionAnalyzer.max_co2_response",
                    message=result
                )

                log.info(f"Sending to topic: {publish_topic}")
                
                # Publish using proper signature:
                self.client.publish_message(
                    protocol=protocol,
                    topic=publish_topic,
                    message=formatted
                )

                log.info(f"Published NEW MAX CO₂ for {vehicle_id}: {formatted}")
                # --- PUBLISH BRANCH END ---

            else:
                log.info(f"CONDITION SKIPPED: CO₂ {co2_val} is not greater than existing max {prev} for vehicle {vehicle_id}")
                log.debug(
                    f"CO₂ {co2_val} <= existing max {prev} for vehicle {vehicle_id}"
                )
            
            log.info("--- END: Message Processing Complete ---")
            
        except Exception as e:
            err = f"Error processing message: {e}"
            log.error(err, exc_info=True)
            # Publish error through SDK
            self.client.publish_error(protocol, err)


# ==========================================================
# CLIENT INITIALIZATION
# ==========================================================
default_handler = EmissionHandler(None)

client = AwsGreengrassPubSubSdkClient(
    BASE_TOPIC,
    default_handler.on_message
)

default_handler.client = client

# Register handler for routing
client.register_message_handler(default_handler)

# Activate IPC + MQTT Pub/Sub
client.activate_ipc_pubsub()
client.activate_mqtt_pubsub()

initialize_firehose_client()

# ==========================================================
# SUBSCRIPTIONS
# ==========================================================
for t in MQTT_SUB_TOPICS:
    log.info(f"Subscribing to {t} via ipc_mqtt")
    client.subscribe_to_topic("ipc_mqtt", t)

log.info("All subscriptions active.")
log.info("Running main loop...")


# ==========================================================
# MAIN LOOP
# ==========================================================
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    log.info("Shutting down...")
