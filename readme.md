Run the following commands in a linux style terminal, such as Git Bash.

# Start cafka cluster

```sh 
docker-compose up -d
```

# Create a topic
It is important to create the topic with the command line interface (cli) before running the producer.
Otherwise, the producer will create the topic without the correct configuration.

```sh 
docker exec broker-1 \
kafka-topics --bootstrap-server broker-2:9092 \
--create \
--topic demo_topic \
--partitions 3 
```

# View existing topics

```sh 
docker exec broker-1 \
kafka-topics --bootstrap-server localhost:9092 \
--list
```

# View detailed info about topic

```sh 
docker exec broker-1 \
kafka-topics --bootstrap-server localhost:9092 \
--describe --topic demo_topic
```

# To reset the kafka cluster, delete the [mnt](mnt) folder
```sh
docker-compose down
rm -rf mnt
```
