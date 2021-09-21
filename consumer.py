# from collections
from kafka.consumer import KafkaConsumer
from kafka.structs import TopicPartition
import redis
import time
import os
import ast
import sys

def main():
    consumer_number = int(sys.argv[1])
    print("cousumer_"+ str(consumer_number))

    topic = os.environ['KAFKA_TOPIC']
    consumer = KafkaConsumer(bootstrap_servers=os.environ['KAFKA_HOST'])
    # topic = "quickstart-events"
    # consumer = KafkaConsumer(bootstrap_servers="192.168.100.10:9092")

    redis_con = redis.Redis(host='192.168.1.6', port=6379, db=0)
    # "topic_info" "{'topic':[[1],[2],[3]]}"
    res = redis_con.get('topic_info')
    # print(ast.literal_eval(res.decode()))
    # print(type(ast.literal_eval(res.decode())))
    
    offset = 0

    print(ast.literal_eval(res.decode()))

    
    partition_list = (ast.literal_eval(res.decode()))[topic]

    while True:
        assign_count = len(partition_list[consumer_number - 1])
        assign_list = list()

        for index in range(assign_count):
            print(partition_list[consumer_number-1][index])
            topic_partitions = TopicPartition(
                    topic, 
                    partition_list[consumer_number-1][index]
            )
            # topic_partitions = {
            #     "topic": topic,
            #     "partition": partition_list[consumer_number-1][index]
            # }
            assign_list.append(
                topic_partitions
            )
        print(assign_list)

        consumer.assign(assign_list)
        start_time = time.time()
        lists = []
        
        for msg in consumer:
            print(msg)
            lists.append(time.time() - ast.literal_eval(msg.value.decode())['time'])
            if (time.time() - start_time) > 10:
                redis_con.set('consumer', str(lists))
                print(lists)
                lists = []
                offset = msg.offset
                print(offset)
                start_time = time.time()
                res = redis_con.get('topic_info')
                partition_list = ast.literal_eval(res.decode())[topic]
                break


if __name__== '__main__':
    main()