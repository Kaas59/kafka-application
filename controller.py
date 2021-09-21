from kafka import KafkaAdminClient
from kafka.admin import NewPartitions
import os
import redis
import time
import json
import ast
import statistics


def main():
    redis_con = redis.Redis(host="192.168.1.6", port=6379, db=0)
    while True:
        time.sleep(1)
        res = statistics.mean(ast.literal_eval(redis_con.get('consumer').decode()))
        print(res)
        if False:
            host = os.environ['KAFKA_HOST']
            print(host)
            clinet_mock = KafkaAdminClient(
                bootstrap_servers = host
            )
            partitions = NewPartitions(
                total_count=5,
                new_assignments=[[0]]
            )
            clinet_mock.create_partitions(
                {"topic": partitions},
                10000,
                False
            )



if __name__== "__main__":
    main()