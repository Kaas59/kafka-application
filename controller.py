from copy import Error
from kafka.admin.client import KafkaAdminClient
from kafka.admin import NewPartitions
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from kafka.admin.new_topic import NewTopic
import os
import redis
import time
import json
import ast
import statistics

# REDIS_HOST = os.environ['REDIS_HOST']
REDIS_HOST = "192.168.1.6"
KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
KAFKA_REDIS_INFO = "topic_info"
KAFKA_NUM_PARTITIONS = 3
THRESHOLD_VALUE = 30.0
MAX_CONSUMER_SERVER = 4
DEFAULT_TIMEOUT_MS = 100000

def main():

    # redisコネクターの設定
    redis_con = redis.Redis(host=REDIS_HOST, port=6379, db=0)
    
    # Redis情報のリセット
    redis_con.set(KAFKA_REDIS_INFO, "{\"topic\":[[0],[1],[2]]}")

    # トピックの作成
    __create_kafka_topic()
    
    # パーティションのサイズを取得
    partition_total = __get_kafka_partitions_size()


    throughput_raw = [0]*partition_total
    new_partition_total = partition_total

    while True:
        time.sleep(1)

        # 各コンシューマーのスループットを取得する
        throughput_raw = __get_throughput(
            redis_con,
            throughput_raw,
            partition_total
        )

        # topicの情報を取得する
        topic_info = __get_topic_info(
            redis_con
        )

        # パーティションの数をチェック(Kafka == Redis)
        partition_list, partition_count = __partiton_count_valid(
            topic_info,
            partition_total
        )
        
        # スループットを分析する
        new_partition_total, partition_list = __throughput_logic(
            partition_count,
            partition_list,
            new_partition_total,
            throughput_raw
        )


        # パーティションの拡張の必要がなければ後続の処理をスキップ
        if new_partition_total <= partition_total:
            continue

        # パーティションの拡張処理
        __add_kafka_patitions(
            new_partition_total
        )
        
        # パーティション拡張にともなうRedisデータ更新
        __set_topic_info(
            redis_con,
            partition_list,
        )

        # パーティション拡張にともなう内部変数の更新
        throughput_raw, partition_total = __update_parms(
            new_partition_total
        )

def __create_kafka_topic():
    consumer = KafkaConsumer(
        bootstrap_servers = KAFKA_HOST
    )
    topics = consumer.topics()
    consumer.close()
    print("Kafkaトピック：",topics)

    if KAFKA_TOPIC in topics:
        print("過去のトピックを削除します。")
        admin_client = KafkaAdminClient(
            bootstrap_servers = KAFKA_HOST
        )
        
        res = admin_client.delete_topics(
            [KAFKA_TOPIC],
            timeout_ms=DEFAULT_TIMEOUT_MS
        )
        print(res)
        admin_client.close()
        time.sleep(3)
    

    print("新規トピックを作成します。")
    admin_client = KafkaAdminClient(
        bootstrap_servers = KAFKA_HOST
    )

    new_topic = NewTopic(
        name = KAFKA_TOPIC,
        num_partitions = KAFKA_NUM_PARTITIONS,
        replication_factor=-1,
        replica_assignments=[],
        topic_configs={}
    )

    res = admin_client.create_topics(
        [new_topic],
        timeout_ms=DEFAULT_TIMEOUT_MS,
        validate_only=False
    )
    print(res)
    admin_client.close()



def __get_kafka_partitions_size():
    
    producer = KafkaProducer(
        bootstrap_servers =KAFKA_HOST
    )

    # 返り値： {0, 1, 2}
    partitions = producer.partitions_for(KAFKA_TOPIC)
    producer.close()

    print(partitions)

    return len(partitions)


def __get_throughput(redis_con, throughput_raw, partition_total):
    for value in range(partition_total):
        try:
            res = redis_con.get("consumer_" + str(value+1))
            throughput_raw[value] = statistics.mean(ast.literal_eval(res.decode()))
        except AttributeError:
            throughput_raw[value] = 0
    
    return throughput_raw


def __partiton_count_valid(topic_info, partition_total):
    partition_list = topic_info[KAFKA_TOPIC]
    partitions = [item for i in partition_list for item in i]
    partition_count = len(partitions)

    if partition_total != (partition_count):
        print("パーティション数(Redis情報)：", partition_count)
        print("パーティション数(Kafka情報)：", partition_total)
        error_message = "パーティション数が誤っています。Kafkaのパーティション数を"+str(partition_count)+"に修正してください。"
        raise ValueError(error_message)

    return partition_list, partition_count


def __throughput_logic(partition_count, partition_list, new_partition_total, throughput_raw):
    print(throughput_raw)

    throughput = list()

    for value in throughput_raw:
        throughput.append(value/sum(throughput_raw)*100)

    print(throughput)

    for index in range(len(throughput)):
        if throughput[index] > THRESHOLD_VALUE:
            partition_list[index].append(partition_count)
            partition_count += 1
    print("※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※\n※ パーティションを拡張します。( %2d -> %2d )※\n※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※" %(new_partition_total, partition_count))

    new_partition_total = partition_count

    return new_partition_total, partition_list


def __add_kafka_patitions(new_partition_total):
    clinet_mock = KafkaAdminClient(
        bootstrap_servers = KAFKA_HOST
    )
    partitions = NewPartitions(
        total_count=new_partition_total,
        new_assignments=[[0]]
    )
    clinet_mock.create_partitions(
        {"topic": partitions},
        DEFAULT_TIMEOUT_MS,
        False
    )


def __get_topic_info(redis_con):
    res = redis_con.get(KAFKA_REDIS_INFO)
    topic_info = ast.literal_eval(res.decode())
    print(topic_info)

    return topic_info
    

def __set_topic_info(redis_con, partition_list):
    redis_con.set(KAFKA_REDIS_INFO, "{\""+str(KAFKA_TOPIC)+"\": "+ str(partition_list) +"}")


def __update_parms(new_partition_total):
    partition_total = new_partition_total
    throughput_raw = [0]*partition_total

    return throughput_raw, partition_total

if __name__== "__main__":
    main()