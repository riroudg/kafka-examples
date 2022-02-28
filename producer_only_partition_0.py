from kafka import KafkaProducer
import json
from data import get_registered_user
import time


### Where is your kafka server ?
kafka_server_port = '172.16.162.103:9092'

### for which topic should program $0 send the events ?
kafka_topic = 'end_user'

### Create a simple json_serializer function
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


### Create a instance producer from Class KafkaProducer
producer = KafkaProducer(
	bootstrap_servers=[kafka_server_port], 
	value_serializer=json_serializer
	)


if __name__ == "__main__":

    ### Sending random lines every 5 seconds
    while True:

        registered_user = get_registered_user()
        print("Topic = end_user, Data = ", json_serializer(registered_user) )
        producer.send(topic=kafka_topic, value=registered_user, partition=0)
        time.sleep(5)


