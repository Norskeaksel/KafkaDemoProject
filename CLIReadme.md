# Download Kafka CLI
Ref: https://kafka.apache.org/quickstart

```sh
wget https://dlcdn.apache.org/kafka/3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
mv  kafka_2.13-3.5.0 kafka
rm kafka_2.13-3.5.0.tgz
cd kafka
```

# Prepare the cluster

```sh
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```

# Start the cluster

```sh 
bin/kafka-server-start.sh config/kraft/server.properties
```

# Create a topic
It is important to create the topic with the command line interface (cli) before running the producer.
Otherwise, the producer will create the topic without the correct configuration.

```sh 
bin/kafka-topics.sh \
--create \
--partitions 3 \
--replication-factor 1 \
--bootstrap-server localhost:9092 \
--topic demo_topic
```

# View existing topics

```sh 
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

# View detailed info about topic

```sh 
 bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic demo_topic
```

# Reset the kafka cluster
```sh
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
```
