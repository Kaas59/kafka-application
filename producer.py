from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
import sys
import os
import json
import time
import random
import ast
import redis
from itertools import chain

REDIS_HOST = os.environ['REDIS_HOST']
KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']

KAFKA_REDIS_INFO = "topic_info"

KAFKA_NUM_PARTITIONS = 3
CONSUMER_SERVER = 4

# THRESHOLD_VALUE = 30.0

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
  partition_info, partition_consumed_total, start_time = __get_partition_consumed_info(redis_con)
  producer_send_partition_count = __reset_producer_send_partition_count()
  __set_partition_produce_info(redis_con, dict())
  partition_producer_total = dict()

  for value in range(10):
    for value in range(1000):
      data_list, partition_id = __send_producer(partition_info, partition_consumed_total, partition_producer_total, producer, value, data_list)
      producer_send_partition_count[str(partition_id+1)] += 1
      
      if (time.time() - start_time) > THROUGHPUT_TIMEOUT:
        producer, producer_send_partition_count, partition_producer_total, partition_info, partition_consumed_total, start_time = __timeout_reset(
          redis_con,
          producer_send_partition_count,
          producer
        )

    data_list = data_list_tmp

      # time.sleep(1)

  # メトリクスの出力
  metrics_output(producer)
  producer.close()

def __timeout_reset(redis_con, producer_send_partition_count, producer):
  # 送信情報を記録
  __set_partition_produce_info(redis_con, producer_send_partition_count)

  # パーティションの情報を再取得
  partition_info, partition_consumed_total, start_time = __get_partition_consumed_info(redis_con)

  # パーティション毎のデータ数を取得
  partition_producer_total = __get_partititons_producer_info(redis_con)

  # プロデューサーの再生成
  producer = __re_create_producer(producer)
  producer_send_partition_count = __reset_producer_send_partition_count()
  return producer, producer_send_partition_count, partition_producer_total, partition_info, partition_consumed_total, start_time


def __send_producer(partition_info, partition_consumed_total, partition_producer_total, producer, value, data_list):
  model_id = 0
  model2_index = 0

  if PRODUCER_NUMBER != 3 and PRODUCER_NUMBER != 4:
    model_id = value % 3
    partition_id = random.choice(partition_info[model_id])
  else:
    model_id = 2
    if len(partition_info[model_id]) == 2:
      model2_index += 1
      count_in_partition = __cal_count_in_partition(partition_info, model_id, partition_producer_total, partition_consumed_total)
      key = 0 if model2_index % 10 <= round(count_in_partition[0]/sum(count_in_partition)*10)  else 1
      partition_id = partition_info[model_id][key]
    else:
      partition_id = partition_info[model_id][0]

  data_list[model_id]["time"] = time.time()
  
  res = producer.send(
    KAFKA_TOPIC,
    key = str(data_list[model_id]["dataModelId"]).encode('utf-8'),
    value = data_list[model_id],
    partition = partition_id
  )

  try:
    result = res.get(timeout=10)
    # print("Value = %d, key = %d, partition = %d, offset = %d" %(model_id, data_list[model_id]["dataModelId"] , partition_id, result.offset))
  except KafkaError:
    error_message = "データの送信中にエラーが発生しました。"
    print(error_message)
    print("※※ Value = %d, key = %d, partition = %d" %(value, value%3 , partition_id))
    # log.exception()
    pass

  data_list[model_id]["dataModelId"] += 1

  return data_list, partition_id

def __cal_count_in_partition(partition_info, model_id, partition_producer_total, partition_consumed_total):
  count_in_partition = list()
  print("partition_producer_total",partition_producer_total)
  print("partition_consumed_total", partition_consumed_total)
  print("partition_info[model_id]", partition_info[model_id])
  for index in partition_info[model_id]:
    count_in_partition.append(partition_producer_total[index] - partition_consumed_total[index])
  print('rate'+str(count_in_partition))
  print("rate max"+str(round(count_in_partition[0]/sum(count_in_partition)*10)))
  print("rate max"+str(round(count_in_partition[1]/sum(count_in_partition)*10)))
  return count_in_partition

def __create_producer():
  return KafkaProducer(
    bootstrap_servers = KAFKA_HOST,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
  )

def __re_create_producer(producer):
  producer.close()
  return __create_producer()

def __get_partition_consumed_info(redis_con):
  # res = redis_con.get(KAFKA_REDIS_INFO)
  # partition_info = ast.literal_eval(res.decode())[KAFKA_TOPIC]
  models_partition = redis_con.get("models_partition")
  partition_info = ast.literal_eval(models_partition.decode())
  partition_consumed_total = dict()
  for index in list(chain.from_iterable(partition_info)):
    try:
      metrics = redis_con.get("metrics_"+str(index))
      print("partition_consumed_total["+str(index)+"]:"+metrics.decode())
      partition_consumed_total[index] = int(metrics.decode())
    except AttributeError:
      partition_consumed_total[index] = 100
  print(partition_info)
  start_time = time.time()

  return partition_info, partition_consumed_total, start_time

def __set_partition_produce_info(redis_con, partition_produce_new):
  print("partition_produce_new",partition_produce_new)
  for key in range(len(partition_produce_new)):
    print("key",key)
    print("partition_produce_new[key+1]",partition_produce_new[str(key+1)])
    redis_key = "partition_"+str(key)+"-producer_"+str(PRODUCER_NUMBER)
    partition_produce_total_tmp = 0
    try:
      partition_produce_total_tmp = int(redis_con.get(redis_key).decode())
      print(type(partition_produce_total_tmp))
      print(partition_produce_total_tmp)
    except AttributeError:
      partition_produce_total_tmp = 0
    redis_con.set(redis_key, str(partition_produce_new[str(key+1)] + partition_produce_total_tmp))


def __get_partititons_producer_info(redis_con):
  partition_producer_total = dict()
  for partition_index in range(5):
    print(partition_index)
    value = 0
    for producer_index in range(5):
      key = "partition_"+str(partition_index)+"-producer_"+str(producer_index+1)
      if redis_con.keys(key):
        value += int(redis_con.get(key).decode())
    partition_producer_total[partition_index] = value
  return partition_producer_total

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

def __reset_producer_send_partition_count():
  return {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}

if __name__ == '__main__':
  main()