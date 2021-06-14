# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 14:30:58 2021

@author: Ferhat
"""

#!usr/bin/python3 #shebang line
import paho.mqtt.client as mqtt #import paho-mqtt as mqtt client
import json #import json library
import random #import random library
import datetime #import datetime library
import time #import time library


def on_message(client, userdata, message):  #callback function to receive every published message
    global subscriber_dict
    subscriber_dict = json.loads(str(message.payload.decode("utf-8"))) #convert received json string format into python dict
    print("message received: ", subscriber_dict) #print received message

broker_address="983072be-6928-4aa5-94db-b538ea35100f.ka.bw-cloud-instance.org" #broker adress
client = mqtt.Client("Manger2") #create new instance
client.on_message=on_message  #attach on_message function to a callback function
client.connect(broker_address) #connect to broker
client.loop_start() #start the client loop to make it always running
client.subscribe("IoTParking/Car") #subscribe to topic
client.subscribe("IoTParking/Garage") #subscribe to topic

try:
    while True:      
        timestamp = str(datetime.datetime.now()) #define timestamp in string format
        rvalue = str(random.randrange(0,10)) #define random value
        publisher_dict = {"value":rvalue, "time":timestamp} #define publisher_dict as python dict
        publisher_json = json.dumps(publisher_dict) #convert publisher_dict into json string format
        client.publish("IoTParking/Assignment",publisher_json) #publish message
        print ("message published: ", publisher_json) #print published message
        time.sleep(5) #sleep or wait for 5s before continuing to the next loop
        
        
except KeyboardInterrupt:
    print("process interrupted by a keyboard input")
    print("process stop")
    client.loop_stop() #stop the client loop
    client.disconnect() # disconnect gracefully client.disconnect() 
