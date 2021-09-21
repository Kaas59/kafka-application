from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
import json
import random
import time
import os

def main():
  topic = os.environ['KAFKA_TOPIC']
  bootstrap_servers = os.environ['KAFKA_HOST']
#   topic = "quickstart-events"
#   bootstrap_servers = "192.168.100.10:9092"
  producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
  )
  data = ["AAA", "BBB", "CCC"]
  while True:
    if time.time() >= float(os.environ['START_TIME']):
      break
  for value in range(100):
    res = producer.send(
      topic,
      key=str(value).encode('utf-8'),
      value={"a": 1234, "time": time.time()},
      # partition=random.choice(range())
      partition=random.choice(range(4))
    )
    try:
      result = res.get(timeout=10)
      print(value, result.partition)
    except KafkaError:
      log.exception()
      pass
    time.sleep(2)
  metrics = producer.metrics()
  print(json.dumps(metrics, indent=2))



if __name__ == '__main__':
  main()