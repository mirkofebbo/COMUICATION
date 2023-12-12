import paho.mqtt.client as paho
from paho import mqtt
import ssl
import csv
import time
from datetime import datetime

# Global variables for caching sensor values
cached_linearAccel = None
cached_magnetometer = None
cached_orientation = None

csv_filename = 'sensor_data.csv'

# write data to CSV
def write_to_csv(data):
    with open(csv_filename, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(data)

# Initialize CSV file with headers
with open(csv_filename, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['unixTime', 'time', 
                        'linearAccel_X', 'linearAccel_Y', 'linearAccel_Z', 
                        'magnetometer_X', 'magnetometer_Y', 'magnetometer_Z', 
                        'orientation_X', 'orientation_Y', 'orientation_Z'])

# MQTT parameters
mqtt_server = "your-hivemq-server-address"
mqtt_port = 8883
mqtt_topics = ["linearAccel", "magnetometer", "orientation"]

# see if we connect.
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

# if publish was successful
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))

# print which topic was subscribed to
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# print message, useful for checking if it was successful
def on_message(client, userdata, msg):
    global cached_linearAccel, cached_magnetometer, cached_orientation

    current_time = time.time()
    human_readable_time = datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')
    payload = msg.payload.decode()

    # Parse the payload to get sensor values
    values = [float(val.split(': ')[1]) for val in payload.split(', ')]

    if msg.topic == "linearAccel":
        cached_linearAccel = values
    elif msg.topic == "magnetometer":
        cached_magnetometer = values
    elif msg.topic == "orientation":
        cached_orientation = values

    # Check if we have both readings available
    if cached_linearAccel is not None and cached_magnetometer is not None and cached_orientation  is not None:
        data = [current_time, human_readable_time] + cached_linearAccel + cached_magnetometer + cached_orientation
        write_to_csv(data)

        # Reset the cache
        cached_linearAccel = None
        cached_magnetometer = None
        cached_orientation = None

# using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
# userdata is user defined data of any type, updated by user_data_set()
# client_id is the given name of the client
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

# enable TLS for secure connection
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
# set username and password
client.username_pw_set("gerolamo", "T&;]x>U]7rk&ecs")
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
client.connect("bd1074d981ef4379ae4d2efbc1be623d.s1.eu.hivemq.cloud", 8883)

# setting callbacks, use separate functions like above for better visibility
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_publish = on_publish

# subscribe to all topics of encyclopedia by using the wildcard "#"
client.subscribe("magnetometer", qos=1)
client.subscribe("linearAccel", qos=1)
client.subscribe("orientation", qos=1)

# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop
client.loop_forever()