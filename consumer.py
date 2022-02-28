from kafka import KafkaConsumer
import json, time, signal
import argparse

def handler(signum, frame):
  msg = "Ctrl-c was pressed."
  print(msg)
  exit(1)


parser = argparse.ArgumentParser()

parser.add_argument("--topic", help="enter a kafka topic")
parser.add_argument("--consumer-group", help="enter a kafka consumer group", dest="consumer_group")

p = parser.parse_args()


### Where is your kafka server ?
kafka_server_port = '172.16.162.103:9092'

### signal.SIGINT = strg + c
signal.signal(signal.SIGINT, handler)

if __name__ == "__main__":

  ### for which topic should program $0 send the events ?
  if not p.topic:
     p.topic = 'registered_user'

  if not p.consumer_group:
     p.consumer_group = 'consumer-group-a'

  consumer = KafkaConsumer(
	p.topic,
	bootstrap_servers=[kafka_server_port],
	auto_offset_reset='earliest',
	group_id=p.consumer_group
	)

  print("starting the kafka consumer in the group '{}' on kafka server '{}'  ... ".format(p.consumer_group, kafka_server_port) )

  counter = 0
  for msg in consumer:

    counter += 1
    if counter % 10 == 1:
      print("Topic = '{}', consumer-group = '{}:".format(p.topic, p.consumer_group))

    print("User = {}".format(json.loads(msg.value)))
    time.sleep(1)


