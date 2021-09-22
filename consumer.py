# from collections
from kafka.consumer import KafkaConsumer
from kafka.structs import TopicPartition
import sys
import time
import os
import ast
import redis


REDIS_HOST = os.environ['REDIS_HOST']
KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
KAFKA_REDIS_INFO = "topic_info"
THROUGHPUT_KEY = "time"
DEFAULT_TIMEOUT_MS = 100000
THROUGHPUT_TIMEOUT = 10
CONSUMER_NUMBER = int(sys.argv[1])

def main():
    print("cousumer_"+ str(CONSUMER_NUMBER))

    redis_con = redis.Redis(host=REDIS_HOST, port=6379, db=0)
    
    offset = 0
    
    consumer_client = KafkaConsumer(
        bootstrap_servers=KAFKA_HOST
    )

    # 購読するパーティションの割当
    consumer_client, start_time, throughput_lists =  __assign_consumer(
        redis_con,
        consumer_client
    )

    while True:
        
        for msg in consumer_client:
            print(msg.partition, msg.offset)

            throughput_lists.append(
                time.time() - ast.literal_eval(msg.value.decode())[THROUGHPUT_KEY]
            )

            if (time.time() - start_time) > THROUGHPUT_TIMEOUT:
                consumer_client.pause()
                offset = msg.offset
                break

        redis_con.set("consumer_"+str(CONSUMER_NUMBER), str(throughput_lists))
        
        print("\nスループット：", throughput_lists)
        print("\nオフセット：", offset)
        consumer_client, start_time, throughput_lists =  __assign_consumer(
            redis_con,
            consumer_client
        )
        consumer_client.poll()


def __get_topic_info(redis_con):
    # "topic_info" "{'topic':[[0],[1],[2]]}"
    res = redis_con.get(KAFKA_REDIS_INFO)
    print(ast.literal_eval(res.decode()))

    partition_list = (ast.literal_eval(res.decode()))[KAFKA_TOPIC]

    return partition_list


def __assign_consumer(redis_con, consumer_client):
    partition_list = __get_topic_info(redis_con)

    assign_count = len(partition_list[CONSUMER_NUMBER - 1])
    assign_list = list()

    for index in range(assign_count):
        # print(partition_list[CONSUMER_NUMBER - 1][index])
        topic_partitions = TopicPartition(
                KAFKA_TOPIC, 
                partition_list[CONSUMER_NUMBER - 1][index]
        )

        assign_list.append(
            topic_partitions
        )
    # print(assign_list)

    consumer_client.assign(assign_list)
    throughput_lists = list()
    start_time = time.time()

    return consumer_client, start_time, throughput_lists

if __name__== '__main__':
    main()