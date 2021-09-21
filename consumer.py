from kafka.consumer import KafkaConsumer
from kafka.structs import TopicPartition
import redis
import time
import os
import ast

def main():
    topic = os.environ['KAFKA_TOPIC']
    print(os.environ['KAFKA_HOST'])
    consumer = KafkaConsumer(bootstrap_servers=os.environ['KAFKA_HOST'])
    # topic = "quickstart-events"
    # consumer = KafkaConsumer(bootstrap_servers="192.168.100.10:9092")
    consumer.assign(
        [
            TopicPartition(topic, 0),
            TopicPartition(topic, 1),
            TopicPartition(topic, 2)
        ]
    )
    start_time = time.time()
    lists = []
    redis_con = redis.Redis(host='192.168.1.6', port=6379, db=0)
    for msg in consumer:
        print(msg)
        lists.append(time.time() - ast.literal_eval(msg.value.decode())['time'])
        if (time.time() - start_time) > 10:
            redis_con.set('consumer', str(lists))
            print(lists)
            lists = []
            start_time = time.time()


if __name__== '__main__':
    main()