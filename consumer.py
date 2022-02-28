from kafka import KafkaConsumer
import json, time, signal
import argparse

def handler(signum, frame):
  msg = "Ctrl-c was pressed."
  print(msg)
  exit(1)


parser = argparse.ArgumentParser()

parser.add_argument("--topic", help="enter a kafka topic")
parser.add_argument("--group-id", help="enter a kafka consumer group-id", dest="group_id")

p = parser.parse_args()


### Where is your kafka server ?
kafka_server_port = '172.16.162.103:9092'

### signal.SIGINT = strg + c
signal.signal(signal.SIGINT, handler)

if __name__ == "__main__":

  ### for which topic should program $0 get the events ?
  if not p.topic:
     p.topic = 'registered_user'

  if not p.group_id:
     p.group_id = 'consumer-group-a'

  consumer = KafkaConsumer(
	p.topic,
	bootstrap_servers=[kafka_server_port],
	auto_offset_reset='earliest',
	group_id=p.group_id
	)

  print("starting the kafka consumer in the group '{}' on kafka server '{}'  ... ".format(p.group_id, kafka_server_port) )

  counter = 0
  for msg in consumer:

    counter += 1
    if counter % 10 == 1:
      print("Topic = '{}', group-id = '{}:".format(p.topic, p.group_id))

    print("User = {}".format(json.loads(msg.value)))
    time.sleep(1)


