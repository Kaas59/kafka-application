from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
import sys
import os
import json
import time
import random
import ast
import redis

REDIS_HOST = os.environ['REDIS_HOST']
KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
KAFKA_REDIS_INFO = "topic_info"
KAFKA_NUM_PARTITIONS = 3
THRESHOLD_VALUE = 30.0
MAX_CONSUMER_SERVER = 4
DEFAULT_TIMEOUT_MS = 100000
THROUGHPUT_TIMEOUT = 10
PRODUCER_NUMBER = int(sys.argv[1])

def main():
  print("Producer_"+ str(PRODUCER_NUMBER))

  # 処理開始を設定する
  while True:
    if time.time() >= float(os.environ['START_TIME']):
      break

  redis_con = redis.Redis(host=REDIS_HOST, port=6379, db=0)

  producer = KafkaProducer(
    bootstrap_servers = KAFKA_HOST,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
  )

  # パーティションの情報を取得
  partition_info, start_time = __get_partition_info(redis_con)

  for value in range(100):
    __send_producer(partition_info, producer, value)
    
    if (time.time() - start_time) > THROUGHPUT_TIMEOUT:
      # パーティションの情報を再取得
      partition_info, start_time = __get_partition_info(redis_con)

    time.sleep(1)

  # メトリクスの出力
  metrics_output(producer)
  producer.close()


def __send_producer(partition_info, producer, value):
  partition_id = random.choice(partition_info[value % 3])
  
  res = producer.send(
    KAFKA_TOPIC,
    key = str(value).encode('utf-8'),
    value = {"data_id": str(value % 3), "time": time.time()},
    partition = partition_id
  )

  try:
    result = res.get(timeout=10)
    print("Value = %d, key = %d, partition = %d, offset = %d" %(value, value%3 , partition_id, result.offset))
  except KafkaError:
    error_message = "データの送信中にエラーが発生しました。"
    print(error_message)
    print("※※ Value = %d, key = %d, partition = %d" %(value, value%3 , partition_id))
    # log.exception()
    pass


def __get_partition_info(redis_con):
  res = redis_con.get(KAFKA_REDIS_INFO)
  partition_info = ast.literal_eval(res.decode())[KAFKA_TOPIC]
  print(partition_info)
  start_time = time.time()

  return partition_info, start_time


def metrics_output(producer):
  metrics = producer.metrics()
  print(json.dumps(metrics, indent=2))


if __name__ == '__main__':
  main()