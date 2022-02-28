#!/usr/bin/env python3
#
# Datei: producer.py
#
# Erstellt 2022-02-25-scma

import json, time, argparse
from kafka import KafkaProducer
from data import get_registered_user
from datetime import datetime

parser = argparse.ArgumentParser()

parser.add_argument("--topic", help="enter a kafka topic", dest="topic")
p = parser.parse_args()

### Where is your kafka server ?
kafka_server_port = '172.16.162.103:9092'

### Create a simple json_serializer function
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


### Create a instance producer from Class KafkaProducer
producer = KafkaProducer(
	bootstrap_servers=[kafka_server_port], 
	value_serializer=json_serializer
	)


if __name__ == "__main__":

    ### for which topic should program $0 send the events ?
    if not p.topic:
      p.topic = 'registered_user'
  
    print("\nstarting the kafka for the topic '{}', sending events to kafka server '{}'  ... \n".format(p.topic, kafka_server_port) )

    ### Sending random lines every 5 seconds
    while True:

        timestamp = str(datetime.now())
        registered_user = timestamp + ': ' + str(get_registered_user())

        print("Topic = {}, Data = {}".format(p.topic, registered_user) )
        producer.send(topic=p.topic, value=registered_user)
        time.sleep(5)


