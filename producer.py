#!/usr/bin/env python3
#
# Datei: producer.py
#
# Erstellt 2022-02-25-scma

from kafka import KafkaProducer
import json
from data import get_registered_user
import time
from datetime import datetime

### Where is your kafka server ?
kafka_server_port = '172.16.162.103:9092'

### for which topic should program $0 send the events ?
kafka_topic = 'registered_user'

### Create a simple json_serializer function
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


### Create a instance producer from Class KafkaProducer
producer = KafkaProducer(
	bootstrap_servers=[kafka_server_port], 
	value_serializer=json_serializer
	)


if __name__ == "__main__":

    print("\nstarting the kafka for the topic '{}', sending events to kafka server '{}'  ... \n".format(kafka_topic, kafka_server_port) )

    ### Sending random lines every 5 seconds
    while True:

        timestamp = str(datetime.now())
        registered_user = timestamp + ': ' + str(get_registered_user())

        #print("Topic = {}, Data = {}".format(kafka_topic, json_serializer(registered_user)) )
        print("Topic = {}, Data = {}".format(kafka_topic, registered_user) )
        producer.send(topic=kafka_topic, value=registered_user)
        time.sleep(5)


