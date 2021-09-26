while true; do
 kafka-consumer-groups.sh --describe --bootstrap-server $KAFKA_HOST --group $KAFKA_GROUP;
done
