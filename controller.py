from kafka import KafkaAdminClient
from kafka.admin import NewPartitions
import os
import redis
import time
import json
import ast
import statistics


def main():
    # partition_total = os.environ['KAFKA_PARTITION_TOTAL']
    partition_total = 3
    redis_con = redis.Redis(host="192.168.1.6", port=6379, db=0)
    # topic = os.environ['KAFKA_TOPIC']
    topic = "topic"
    throughput_raw = [0]*partition_total
    while True:
        time.sleep(1)
        for value in range(partition_total):
            try:
                res = redis_con.get("consumer_"+str(value+1))
                throughput_raw[value] = statistics.mean(ast.literal_eval(res.decode()))
            except AttributeError:
                throughput_raw[value] = 0
            
        
        new_partition_total, partition_list = throughput_logic(topic, redis_con, partition_total,throughput_raw)

        if new_partition_total > partition_total:
            host = os.environ['KAFKA_HOST']
            topic_info = redis_con.get('topic_info')
            print(topic_info)
            print(host)

            clinet_mock = KafkaAdminClient(
                bootstrap_servers = host
            )
            partitions = NewPartitions(
                total_count=new_partition_total,
                new_assignments=[[0]]
            )
            clinet_mock.create_partitions(
                {"topic": partitions},
                10000,
                False
            )
            
            redis_con.set("topic_info","{\""+str(topic)+"\": "+ str(partition_list) +"}")
            partition_total = new_partition_total
            throughput_raw = [0]*partition_total

def throughput_logic(topic, redis_con,partition_total,throughput_raw):
    print(throughput_raw)
    new_partition_total = 0

    if partition_total == 3:
        new_partition_total = partition_total + 1
        res = redis_con.get('topic_info')
        # partition_list = ast.literal_eval(res.decode())[topic]
        partition_list = [[0,3],[1],[]]

    return new_partition_total, partition_list


if __name__== "__main__":
    main()