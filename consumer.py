from kafka import KafkaConsumer
import json
import time
import signal


def handler(signum, frame):
  msg = "Ctrl-c was pressed."
  print(msg)
  exit(1)


### Where is your kafka server ?
kafka_server_port = '172.16.162.103:9092'

### for which topic should program $0 send the events ?
kafka_topic = 'registered_user'

### signal.SIGINT = strg + c
signal.signal(signal.SIGINT, handler)

if __name__ == "__main__":

  consumer = KafkaConsumer(
	kafka_topic,
	bootstrap_servers=[kafka_server_port],
	auto_offset_reset='earliest',
	group_id="consumer-group-a"
	)

  print("starting the kafka consumer for the topic '{}' on kafka server '{}'  ... ".format(kafka_topic, kafka_server_port) )

  for msg in consumer:

    print("Topic = {}, User = {}".format(kafka_topic, json.loads(msg.value)))
    time.sleep(1)


