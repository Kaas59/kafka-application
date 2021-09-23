# Kafka Application

## ライブラリ
- kafka-python
- redis
- statistics

```bash
$ pip3 install kafka-python redis statistics
```

## 起動コマンド

```bash
# ①Redis起動
$ docker-compose up

# ②コントローラー (オプション 1：トピックを作り直す, 0：トピックをそのまま使用する)
$ python3 controller.py 1

# ③コンシューマー (オプション 1~n：サーバーのNo)
$ python3 consumer.py

# ④プロデューサー (オプション 1~4：サーバーのNo)
$ python3 producer.py

```


## 必須環境変数

- REDIS_HOST => 192.168.1.6
- KAFKA_HOST => 192.168.1.2:9029
- KAFKA_TOPIC => topic
- START_TIME => 0

```bash
# ~/.bashrc or ~/.zshrc

export REDIS_HOST=192.168.1.6
export KAFKA_TOPIC=topic
export KAFKA_HOST=192.168.1.2:9092
export START_TIME=0
```

# コマンドリスト

## TOPICの操作

- Topicの作成

```bash
$ bin/kafka-topics.sh --create --zookeeper 192.168.1.2:2181 --topic topic
```

- Topicの削除

```bash
$ bin/kafka-topics.sh --delete --zookeeper 192.168.1.2:2181 --topic topic
```

- Topicのリスト取得

```bash
$ bin/kafka-topics.sh --list --zookeeper 192.168.1.2:2181
```

- Topicの詳細情報取得

```bash
$ bin/kafka-topics.sh --describe --zookeeper 192.168.1.2:2181 --topic topic
```

## Consumer Groupの操作

- Consumer Groupの作成

```bash
$ bin/kafka-consumer-groups.sh --create --bootstrap-server 192.168.1.2:9092 --group consumer
```

- Consumer Groupの削除

```bash
$ bin/kafka-consumer-groups.sh --delete --bootstrap-server 192.168.1.2:9092 --group consumer
```

- Consumer Group Offsetのリセット

```bash
$ bin/kafka-consumer-groups.sh --reset-offsets --bootstrap-server 192.168.1.2:9092 --group consumer  --to-earliest --all-topics --execute
```

- Consumer Groupのリスト取得

```bash
$ bin/kafka-consumer-groups.sh --list --bootstrap-server 192.168.1.2:9092
```

- Consumer Groupの詳細情報取得

```bash
$ bin/kafka-consumer-groups.sh --describe --bootstrap-server 192.168.1.2:9092 --group consumer
```

# 手順書

## Kafkaセットアップ

```bash
$ cd ~

$ wget https://archive.apache.org/dist/kafka/2.4.0/kafka_2.11-2.4.0.tgz

$ tar -zxvf kafka_2.11-2.4.0.tgz

$ cd kafka_2.11-2.4.0

$ bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

$ bin/kafka-server-start.sh -daemon config/server.properties
```

※参考資料(https://kazuhira-r.hatenablog.com/entry/20170225/1488016716)

## オリジナル環境変数&エイリアス(.bashrc)

```bash
# Kafka
export PATH=$PATH:/home/$USERNAME/kafka_2.11-2.4.0/bin
export REDIS_HOST=192.168.1.6
export KAFKA_TOPIC=topic
export KAFKA_HOST=192.168.1.2:9092
export KAFKA_GROUP=consumer
export ZOOKEEPER_HOST=192.168.1.2:2181
export START_TIME=0

alias kafka-group-offset-reset='kafka-consumer-groups.sh --reset-offsets --bootstrap-server $KAFKA_HOST --group $KAFKA_GROUP --to-earliest --all-topics --execute'
alias kafka-group-list='kafka-consumer-groups.sh --list --bootstrap-server $KAFKA_HOST'
alias kafka-group-describe='kafka-consumer-groups.sh --describe --bootstrap-server $KAFKA_HOST --group $KAFKA_GROUP'
alias kafka-delete-topic='kafka-topics.sh --delete --zookeeper $ZOOKEEPER_HOST --topic $KAFKA_TOPIC'
alias kafka-list-topic='kafka-topics.sh --list --zookeeper $ZOOKEEPER_HOST'
alias kafka-describe-topic='kafka-topics.sh --describe --zookeeper $ZOOKEEPER_HOST --topic $KAFKA_TOPIC'

```