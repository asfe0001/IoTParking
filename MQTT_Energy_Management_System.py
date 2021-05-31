#!python3

######################################################################################################
# Adapted by Danial Haris Limi Hawari, LiDa17, dannyalharris@gmail.com                               #
# MQTT Energy Management System for IKKU Lab 043, Hochschule Karlsruhe, HsKA                         #
# This script can be run on terminal. For instance, python MQTT_Energy_Management_System.py -h       #
# See below to see command line usage                                                                #
######################################################################################################


import paho.mqtt.client as mqtt
import json
import os
import datetime
import time
import sys, getopt, random
import logging
from MQTT_Database_Logger import MQTT_SQLite_Logger
import threading
from queue import Queue
import re


#### Use DEBUG,INFO,WARNING
q=Queue() #Queue messages inside python dictionary the prevent bottleneck
mqttclient_log=False #MQTT client logs showing messages
logging.basicConfig(level=logging.INFO) #Error logging
settings=dict()
last_message=dict()
subscriber_dict=dict()
####

#### User configurable data section

brokers=["localhost","192.168.178.20",
         "test.mosquitto.org","broker.hivemq.com","iot.eclipse.org"] # Public open broker to test
# Set MQTT broker IP adress, whether VerneMQ broker or public online broker

settings["broker"]=brokers[0]
settings["username"]="IKKULab043" # MQTT broker username, VerneMQ broker username in our case
settings["password"]="IKKULab000" # MQTT broker password, VerneMQ broker password in our case
settings["port"]=1883 # MQTT broker port
settings["verbose"]=False # Set True to print all messages, False to print only changed messages
settings["clientname"]="" # Set client name or this instance name # settings["clientname"]="IKKULab043EMS"
settings["keepalive"]=60 # Set timeout when there is no connection, in this case 60 seconds
settings["topics"]=[("HsKA/IKKULab/IKKULab043/CPS/#",2)] # Example below

# settings["topics"]=[("HsKA/IKKULab/IKKULab043/CPS/#",0),("HsKA/IKKULab/IKKULab043/CPS/CHP",0),
#           ("HsKA/IKKULab/IKKULab043/CPS/Buffer/Monitoring/Sensor/TempSensor/Temperature",0),
#           ("HsKA/IKKULab/IKKULab043/CPS/Consumer/Controlling/Actuator/HeatElement/Parameter",0),
#           ("HsKA/IKKULab/IKKULab043/CPS/ECar/Monitoring/#",0)]
# settings["topics"]=[("HsKA/#",0)]
# settings["topics"]=[("HsKA/IKKULab/IKKULab043/CPS/+/Monitoring/#",0)]
# settings["topics"]=[("house/bulbs/bulb1",0)]

# Set topics that we would like to subscribe together with their QoS level, 0, 1, 2.
# You can use '# ' or '+' as a wildcard to subscribe multilevel topics or all topics at the same level
# topics at the same time

#### Set SQLite
db_file="MQTT-EMS.db"

class DBTABLE:
    def __init__(self,table_name,table_fields):
        self.table_name= table_name
        self.table_fields=  { "ID":"integer primary key autoincrement",
                            "Timestamp":"text",
                            "Topic":"text",
                            "Publisher":"text",
	                        "SystemType": "text",
	                        "Purpose": "text",
                            "ParameterValue": "text",
	                        "Remark": "text",
                            }

#### Declare and create table in the database

CHP=DBTABLE("CHP","CHP")
Buffer=DBTABLE("Buffer","Buffer")
ECar=DBTABLE("ECar","Buffer")
Consumer=DBTABLE("Consumer","Consumer")
MYITOPS=DBTABLE("MYITOPS","MYITOPS")
Miscellaneous=DBTABLE("Miscellaneous","Miscellaneous")


#### Command line usage that can be use to run directly on terminal
# For example, python MQTT_Energy_Management_System.py -b 192.168.178.20 -t IKKULab043/CPS/#
def command_input(settings={}):
    topics_in=[]
    qos_in=[]

    valid_options=" -b <broker> -p <port> -t <topic> -q QOS -v <verbose> -h <help>\
-c <loop time secs -d logging debug  -n client id or name\
-i loop Interval -u Username -p password\
"
    print_options_flag=False

    try:
      opts, args = getopt.getopt(sys.argv[1:],"h:b:i:d:k:p:t:q:l:v:n:u:p:")
    except getopt.GetoptError:
      print (sys.argv[0],valid_options)
      sys.exit()
    qos=0

    for opt, arg in opts:
        if opt == '-h':
            print (sys.argv[0],valid_options)
            sys.exit()
        elif opt == "-b":
            settings["broker"] = str(arg)
        elif opt == "-i":
            settings["interval"] = int(arg)
        elif opt == "-k":
            settings["keepalive"] = int(arg)
        elif opt =="-p":
            settings["port"] = int(arg)
        elif opt =="-t":
            topics_in.append(arg)
        elif opt =="-q":
            qos_in.append(int(arg))
        elif opt =="-n":
            settings["clientname"]=arg
        elif opt =="-d":
            settings["loglevel"]="DEBUG"
        elif opt == "-p":
            settings["password"] = str(arg)
        elif opt == "-u":
            settings["username"] = str(arg)
        elif opt =="-v":
            settings["verbose"]=True
      

    lqos=len(qos_in)
    for i in range(len(topics_in)):
        if lqos >i: 
            topics_in[i]=(topics_in[i],int(qos_in[i]))
        else:
            topics_in[i]=(topics_in[i],0)         
        
    if topics_in:
        settings["topics"]=topics_in # array with qos

####

#### callbacks -all others define in functions module
def on_connect(client, userdata, flags, rc):
    logging.debug("Connected flags"+str(flags)+"result code "\
    +str(rc)+"client1_id")
    if rc==0:
        client.connected_flag=True
    else:
        client.bad_connection_flag=True

def on_disconnect(client, userdata, rc):
    logging.debug("disconnecting reason " + str(rc))
    client.connected_flag=False
    client.disconnect_flag=True
    client.subscribe_flag=False
    
def on_subscribe(client,userdata,mid,granted_qos):
    m="in on subscribe callback result "+str(mid)
    logging.debug(m)
    client.subscribed_flag=True

def on_message(client,userdata, msg):
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    message_handler(client,m_decode,topic)

def message_handler(client,msg,topic):
    # Converting datetime object to string
    timestampStr = str(datetime.datetime.now())
    m = timestampStr + " " + topic + " " + msg
    subscriber_dict=json.loads(msg) # Convert json message
    subscriber_dict["Topic"]=topic  # into python dict
    subscriber_dict["Timestamp"] = timestampStr

    # Ignore same incoming message
    if has_changed(topic,msg):
        print("storing changed data",topic, "   ",msg)
        q.put(subscriber_dict) # put messages on queue

def has_changed(topic,msg):
    topic2=topic.lower()
    if topic in last_message:
        if last_message[topic]==msg:
            return False
    last_message[topic]=msg
    return True

def log_worker():
    # runs in own thread to log data
    # create logger
    logger = MQTT_SQLite_Logger(db_file)
    # logger.drop_table(CHP.table_name) # Delete or drop table from the database
                                        # for everytime running this script
    logger.create_table(CHP.table_name, CHP.table_fields)
    logger.create_table(Buffer.table_name, Buffer.table_fields)
    logger.create_table(ECar.table_name, ECar.table_fields)
    logger.create_table(Consumer.table_name, Consumer.table_fields)
    logger.create_table(MYITOPS.table_name, MYITOPS.table_fields)
    logger.create_table(Miscellaneous.table_name, Miscellaneous.table_fields)

    while Log_worker_flag:
        while not q.empty():
            subscriber_dict = q.get()
            Timestamp = subscriber_dict["Timestamp"]
            Topic = subscriber_dict["Topic"]
            Publisher = subscriber_dict["Publisher"]
            SystemType = subscriber_dict["SystemType"]
            Purpose = subscriber_dict["Purpose"]
            ParameterValue = subscriber_dict["ParameterValue"]
            Remark = subscriber_dict["Remark"]
            data_out = [Timestamp, Topic, Publisher, SystemType, Purpose, \
                        ParameterValue, Remark]

            if subscriber_dict is None:
                continue

            try:
                if re.search("CHP", subscriber_dict["Topic"]):
                    # Insert subscribed data into the SQLite DB
                    data_query = "INSERT INTO " + \
                             CHP.table_name + "(Timestamp,Topic,Publisher,SystemType,Purpose,\
            		ParameterValue,Remark)VALUES(?,?,?,?,?,?,?)"
                    logger.Log_input(data_query, data_out)
                elif re.search("Buffer", subscriber_dict["Topic"]):
                        # Insert subscribed data into the SQLite DB
                    data_query = "INSERT INTO " + \
                                     Buffer.table_name + "(Timestamp,Topic,Publisher,SystemType,Purpose,\
                            		ParameterValue,Remark)VALUES(?,?,?,?,?,?,?)"
                    logger.Log_input(data_query, data_out)
                elif re.search("ECar", subscriber_dict["Topic"]):
                        # Insert subscribed data into the SQLite DB
                    data_query = "INSERT INTO " + \
                                     ECar.table_name + "(Timestamp,Topic,Publisher,SystemType,Purpose,\
                            		ParameterValue,Remark)VALUES(?,?,?,?,?,?,?)"
                    logger.Log_input(data_query, data_out)
                elif re.search("Consumer", subscriber_dict["Topic"]):
                        # Insert subscribed data into the SQLite DB
                    data_query = "INSERT INTO " + \
                                     Consumer.table_name + "(Timestamp,Topic,Publisher,SystemType,Purpose,\
                            		ParameterValue,Remark)VALUES(?,?,?,?,?,?,?)"
                    logger.Log_input(data_query, data_out)
                if re.search("MYITOPS", subscriber_dict["Topic"]):
                        # Insert subscribed data into the SQLite DB
                    data_query = "INSERT INTO " + \
                                     MYITOPS.table_name + "(Timestamp,Topic,Publisher,SystemType,Purpose,\
                            		ParameterValue,Remark)VALUES(?,?,?,?,?,?,?)"
                    logger.Log_input(data_query, data_out)

                else:
                    # Insert subscribed data into the SQLite DB
                    data_query = "INSERT INTO " + \
                                 Miscellaneous.table_name + "(Timestamp,Topic,Publisher,SystemType,Purpose,\
                                               		ParameterValue,Remark)VALUES(?,?,?,?,?,?,?)"
                    logger.Log_input(data_query, data_out)

            except Exception as e:
                print("problem with logging ", e)

    logger.conn.close()

            # print("message saved ")
####

####
def Initialise_clients(clientname,cleansession=True):
    # flags set
    client= mqtt.Client(clientname)
    if mqttclient_log: # enable mqqt client logging
        client.on_log=on_log
    client.on_connect=on_connect        # attach function to callback
    client.on_message=on_message        # attach function to callback
    client.on_disconnect=on_disconnect
    client.on_subscribe=on_subscribe
    return client
####

####

def convert(t):
    d=""
    for c in t:  # replace all chars outside BMP with a !
            d =d+(c if ord(c) < 0x10000 else '!')
    return(d)

def print_out(m):
    if display:
        print(m)

#### Main program
if __name__ == "__main__" and len(sys.argv)>=2:
    command_input(settings)
    pass

verbose=settings["verbose"]

if not settings["clientname"]:
    r=random.randrange(1,10000)
    clientname="IKKULabEMS-"+str(r)
else:
    clientname="IKKULabEMS-"+str(settings["clientname"])

#### Initialise_client_object() #  add extra flags

logging.info(" creating client "+clientname)
client=Initialise_clients(clientname,False) # create and initialise client object

if settings["username"] !="":
    print("setting username:",settings["username"] )
    client.username_pw_set(settings["username"], settings["password"])

####
t = threading.Thread(target=log_worker) # start logger
Log_worker_flag=True
t.start() # start logging thread
#####

client.connected_flag=False #  flag for connection
client.bad_connection_flag=False
client.subscribed_flag=False
client.loop_start()
client.connect(settings["broker"],settings["port"])
# client.connect(settings["broker"], settings["port"], settings["keepalive"]))

while not client.connected_flag: # wait for connection
    print("waiting for connection")
    time.sleep(1)

client.subscribe(settings["topics"])

while not client.subscribed_flag: # wait to subscribe to topic
    print("waiting for subscribe")
    time.sleep(1)

print("subscribed topic:",settings["topics"])

#### loop and wait until interrupted
try:
    while True:
        pass
except KeyboardInterrupt:
    print("interrrupted by keyboard")

client.loop_stop()  # final check for messages
time.sleep(5)
Log_worker_flag=False # stop logging thread

print("program stop")
