from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
from kafka.admin import NewPartitions
from kafka.admin.new_topic import NewTopic
from kafka.admin.client import KafkaAdminClient
import sys
import os
import time
import ast
import redis
import statistics

REDIS_HOST = os.environ['REDIS_HOST']
KAFKA_HOST = os.environ['KAFKA_HOST']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']

KAFKA_REDIS_INFO = "topic_info"
HAS_EXTENDED_FLAG = True

KAFKA_NUM_PARTITIONS = 4
MAX_CONSUMER_SERVER = 4
MAX_PARTITIONS = 5

THRESHOLD_VALUE = 30.0

DEFAULT_TIMEOUT_MS = 100000

CREATE_FLAG = int(sys.argv[1])


def main():

    # データモデルとサーバーリソースの関係
    models_resource = [[1,2], [3], [4]]
    models_partition = [[0,1],[2],[3]]

    # redisコネクターの設定
    redis_con = redis.Redis(host=REDIS_HOST, port=6379, db=0)
    
    # Redis情報のリセット
    redis_con.set(KAFKA_REDIS_INFO, "{\"topic\":[[0],[1],[2],[3]]}")
    redis_con.set("models_partition", str(models_partition))

    # トピックの作成
    if CREATE_FLAG:
        __create_kafka_topic()
    
    # パーティションのサイズを取得
    partition_total = __get_kafka_partitions_size()
    print("partition_total=",partition_total)


    throughput_raw = [0]*partition_total
    new_partition_total = partition_total
    time.sleep(9)

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
        new_partition_total, partition_list, models_resource, models_partition = __throughput_logic(
            models_resource,
            partition_count,
            partition_list,
            new_partition_total,
            throughput_raw,
            models_partition
        )

        # パーティションの拡張処理
        if (new_partition_total > partition_total):
            __add_kafka_partitions(
                new_partition_total
            )
        
        # Redisデータ更新
        __set_topic_info(
            redis_con,
            partition_list,
            models_partition
        )

        # 内部変数の更新
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
    print("トピックの作成完了")



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
            res = redis_con.get("partition_" + str(value))
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


def __throughput_logic(models_resource, partition_count, partition_list, new_partition_total, throughput_raw, models_partition):
    print(throughput_raw)

    # スループットによる分析ロジック
    partitions_throughput = list()
    throughput = list()

    for value in throughput_raw:
        partitions_throughput.append(value/sum(throughput_raw)*100)

    print(partitions_throughput)

    for servers_list in models_resource:
        # print("sample start")
        # print(servers_list)
        # print([consumer for consumer in servers_list])
        # print([partition for consumer in servers_list for partition in partition_list[consumer - 1]])
        # print([partitions_throughput[partition] for consumer in servers_list for partition in partition_list[consumer - 1]])
        # print("sample end")
        throughput.append(statistics.mean([partitions_throughput[partition] for consumer in servers_list for partition in partition_list[consumer - 1]]))
        

    models_resource_count_list = [len(value) for value in models_resource]
    model_max_index = models_resource_count_list.index(max(models_resource_count_list))
    print("model_max_index=", model_max_index)

    # データモデルとサーバーリソースの関係
    # models_resource = [[1,2], [3], [4]]
    # models_partition = [[0,1],[2],[3]]

    # パーティションとコンシューマーの割当変更
    if max(throughput) > THRESHOLD_VALUE:
        if partition_count < MAX_PARTITIONS and HAS_EXTENDED_FLAG:
            # 拡張すべきモデルを抽出
            throughput_max_index = throughput.index(max(throughput))
            # 新規パーティションの割当(実際に宛先が変更されるもの)
            models_partition[throughput_max_index].append(partition_count)
            # リソースの再割当(管理上)
            models_resource[throughput_max_index].append(models_resource[model_max_index].pop(-1))
            # consumer側の読み取りパーティション設定
            partition_list[model_max_index].append(partition_list[1].pop(0))
            partition_list[1].append(partition_count)
            partition_count += 1
            
            # print("sample start")
            # print(servers_list)
            # print([consumer for consumer in servers_list])
            # print([partition for consumer in servers_list for partition in partition_list[consumer - 1]])
            # print([partitions_throughput[partition] for consumer in servers_list for partition in partition_list[consumer - 1]])
            # print("sample end")

        print("models_resource=", models_resource)
        print("partition_list=",partition_list)
        print("models_partition=",models_partition)
    
    
    if partition_count > new_partition_total:
        print("※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※\n※ パーティションを拡張します。( %2d -> %2d )※\n※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※ ※" %(new_partition_total, partition_count))

    new_partition_total = partition_count

    return new_partition_total, partition_list, models_resource, models_partition


def __add_kafka_partitions(new_partition_total):
    client_mock = KafkaAdminClient(
        bootstrap_servers = KAFKA_HOST
    )
    partitions = NewPartitions(
        total_count=new_partition_total,
        new_assignments=[[0]]
    )
    client_mock.create_partitions(
        {"topic": partitions},
        DEFAULT_TIMEOUT_MS,
        False
    )


def __get_topic_info(redis_con):
    res = redis_con.get(KAFKA_REDIS_INFO)
    topic_info = ast.literal_eval(res.decode())
    print(topic_info)

    return topic_info
    

def __set_topic_info(redis_con, partition_list, models_partition):
    redis_con.set(KAFKA_REDIS_INFO, "{\""+str(KAFKA_TOPIC)+"\": "+ str(partition_list) +"}")
    redis_con.set("models_partition", str(models_partition))


def __update_parms(new_partition_total):
    partition_total = new_partition_total
    throughput_raw = [0]*partition_total

    return throughput_raw, partition_total

if __name__== "__main__":
    main()