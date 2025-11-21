# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np
import sys


#TODO 1: modify the following parameters
#Starting and end index, modify this
device_st = 1
device_end = 5

#Path to the dataset, modify this
# NOTE: Ensure this data/ directory and files exist on your EC2 instance
data_path = "data/vehicle{}.csv" 

#Path to your certificates, modify this
certificate_formatter = "thing-private/MyThing-{}-certificate.pem.crt"
key_formatter = "thing-private/MyThing-{}-private.pem.key"


class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        
        # The unique topic this device will listen for results on
        self.results_topic = "vehicle/results/veh{}/max_co2".format(self.device_id)
        
        #TODO 2: modify your broker address
        # This endpoint is typically your IoT Core Data Endpoint (or Greengrass Discovery endpoint if using V2 SDK)
        self.client.configureEndpoint("a1ew3r7nm48e6x-ats.iot.us-east-2.amazonaws.com", 8883)
        self.client.configureCredentials("AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1) # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2) # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(30) # 10 sec
        self.client.configureMQTTOperationTimeout(5) # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        #TODO 3: fill in the function to show your received message
        payload = message.payload.decode("utf-8")
        topic = message.topic
        
        # Enhanced message output for debugging
        if "vehicle/results" in topic:
            print("\n=======================================================")
            print("RESULT RECEIVED! (Client: {})".format(self.device_id))
            print("Topic: {}".format(topic))
            print("Payload: {}".format(payload))
            print("=======================================================\n")
        else:
             print("client {} received payload {} from topic {}".format(self.device_id, payload, topic))


    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass


    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass


    def publish(self, topic="vehicle/emission/data"):
    # Load the vehicle's emission data
        try:
            # Load data from the device-specific file path
            df = pd.read_csv(data_path.format(self.device_id))
        except FileNotFoundError:
            print(f"ERROR: Data file not found for device {self.device_id} at {data_path.format(self.device_id)}", file=sys.stderr)
            return
        
        for index, row in df.iterrows():
            raw_data = row.to_dict()
            msg_id = str(int(time.time() * 1000)) 
            
            formatted_message_dict = {
                "sdk_version": "0.1.4", 
                "message_id": msg_id,
                "status": 200,      
                "route": topic,  
                "message": raw_data   
            }

            payload = json.dumps(formatted_message_dict)
            
            # Publish the structured payload
            print(f"Publishing: {payload} to {topic}")
            self.client.publish(topic, payload, 1)
            
            # Sleep to simulate real-time data publishing
            time.sleep(0.005)

            
            
            
# --- MAIN EXECUTION START ---

print("Loading vehicle data...")
# Note: Data loading section only loads first 5 vehicle data for quick check, 
# not the full 499 used later. This is okay for simulation speed.
data = []
for i in range(5):
    try:
        a = pd.read_csv(data_path.format(i))
        data.append(a)
    except FileNotFoundError:
        print(f"Warning: Could not load data/vehicle{i}.csv")

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    print("CLIENT ", device_id, end=" ")
    
    # 1. Initialize the client (connect() is NOT called in __init__)
    client = MQTTClient(
        device_id,
        certificate_formatter.format(device_id),
        key_formatter.format(device_id)
    )
    
    # 2. Connect the client
    client.client.connect() 
    
    # 3. Subscribe the client to its unique results topic
    print("Subscribing to results topic: {}".format(client.results_topic))
    # QoS 1 ensures reliable delivery of results
    client.client.subscribe(client.results_topic, 1, client.customSubackCallback)
    
    clients.append(client)
    if device_id % 30 == 0:
        time.sleep(1) # small pause to ease network load
    else:
        sys.stdout.write(".")
        sys.stdout.flush()

print("\n--- All clients initialized and subscribed. Ready to send data. ---")

while True:
    print("send now? (s: send data, d: disconnect and exit)")
    x = input().strip().lower()
    
    if x == "s":
        print("Starting data publication for all clients...")
        for i,c in enumerate(clients):
            c.publish()
        print("--- All clients finished publishing data for this cycle. ---")

    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        exit()
    else:
        print("wrong key pressed. Try 's' or 'd'.")

    time.sleep(3)