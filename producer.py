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
CONSUMER_SERVER = 4

THRESHOLD_VALUE = 30.0

DEFAULT_TIMEOUT_MS = 100000
THROUGHPUT_TIMEOUT = 1

PRODUCER_NUMBER = int(sys.argv[1])


def main():

  data_list_tmp = json_load()
  data_list = data_list_tmp
  print("Producer_"+ str(PRODUCER_NUMBER))

  # 処理開始を設定する
  while True:
    if time.time() >= float(os.environ['START_TIME']):
      break

  redis_con = redis.Redis(host=REDIS_HOST, port=6379, db=0)

  producer = __create_producer()

  # パーティションの情報を取得
  partition_info, start_time = __get_partition_info(redis_con)

  for value in range(10):
    for value in range(1000):
      data_list = __send_producer(partition_info, producer, value, data_list)
      
      if (time.time() - start_time) > THROUGHPUT_TIMEOUT:
        # パーティションの情報を再取得
        partition_info, start_time = __get_partition_info(redis_con)

        # プロデューサーの再生成
        producer = __re_create_producer(producer)

    data_list = data_list_tmp

      # time.sleep(1)

  # メトリクスの出力
  metrics_output(producer)
  producer.close()


def __send_producer(partition_info, producer, value, data_list):
  model_id = 0

  if PRODUCER_NUMBER != 3:
    model_id = value % 3
  else:
    model_id = 2
  
  partition_id = random.choice(partition_info[model_id])
  data_list[model_id]["time"] = time.time()
  
  res = producer.send(
    KAFKA_TOPIC,
    key = str(data_list[model_id]["dataModelId"]).encode('utf-8'),
    value = data_list[model_id],
    partition = partition_id
  )

  try:
    result = res.get(timeout=10)
    print("Value = %d, key = %d, partition = %d, offset = %d" %(model_id, data_list[model_id]["dataModelId"] , partition_id, result.offset))
  except KafkaError:
    error_message = "データの送信中にエラーが発生しました。"
    print(error_message)
    print("※※ Value = %d, key = %d, partition = %d" %(value, value%3 , partition_id))
    # log.exception()
    pass

  data_list[model_id]["dataModelId"] += 1

  return data_list

def __create_producer():
  return KafkaProducer(
    bootstrap_servers = KAFKA_HOST,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
  )

def __re_create_producer(producer):
  producer.close()
  return __create_producer()

def __get_partition_info(redis_con):
  # res = redis_con.get(KAFKA_REDIS_INFO)
  # partition_info = ast.literal_eval(res.decode())[KAFKA_TOPIC]
  res = redis_con.get("models_partition")
  partition_info = ast.literal_eval(res.decode())
  print(partition_info)
  start_time = time.time()

  return partition_info, start_time


def metrics_output(producer):
  metrics = producer.metrics()
  print(json.dumps(metrics, indent=2))


def json_load():
  pass
  model_1 = json.load(open('./real-sample-data/jr_operation.json', 'r'))
  model_2 = json.load(open('./real-sample-data/metro_operation.json', 'r'))
  model_3 = json.load(open('./real-sample-data/user_device.json', 'r'))
  data_list = [model_1, model_2, model_3]
  return data_list


if __name__ == '__main__':
  main()