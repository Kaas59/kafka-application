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
KAFKA_GROUP = "consumer"
DEFAULT_TIMEOUT_MS = 100000
THROUGHPUT_TIMEOUT = 10
CONSUMER_NUMBER = int(sys.argv[1])

def main():
    print("cousumer_"+ str(CONSUMER_NUMBER))

    redis_con = redis.Redis(host=REDIS_HOST, port=6379, db=0)
        
    consumer_client = KafkaConsumer(
        bootstrap_servers = KAFKA_HOST,
        group_id = KAFKA_GROUP
    )

    # 購読するパーティションの割当
    consumer_client, start_time, throughput_dict =  __assign_consumer(
        redis_con,
        consumer_client
    )

    while True:
        
        for msg in consumer_client:
            print("partition=%d, offset=%d" %(msg.partition, msg.offset))

            throughput_dict[msg.partition].append(
                time.time() - ast.literal_eval(msg.value.decode())[THROUGHPUT_KEY]
            )

            if (time.time() - start_time) > THROUGHPUT_TIMEOUT:
                consumer_client.pause()
                offset = msg.offset
                break
        
        for key in throughput_dict.keys():
            redis_con.set("partition_"+str(key), str(throughput_dict[key]))
        
        print("\nスループット：", throughput_dict)
        print("オフセット：", offset)
        consumer_client, start_time, throughput_dict =  __assign_consumer(
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
    throughput_dict = dict()

    for index in range(assign_count):
        topic_partitions = TopicPartition(
                KAFKA_TOPIC, 
                partition_list[CONSUMER_NUMBER - 1][index]
        )

        assign_list.append(
            topic_partitions
        )

        throughput_dict[partition_list[CONSUMER_NUMBER -1][index]] = list()


    consumer_client.assign(assign_list)
    start_time = time.time()

    return consumer_client, start_time, throughput_dict

if __name__== '__main__':
    main()