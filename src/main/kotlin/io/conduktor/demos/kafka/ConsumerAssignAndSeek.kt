package io.conduktor.demos.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.*


object ConsumerDemoAssignSeek {
    @JvmStatic
    fun main(args: Array<String>) {
        val log = LoggerFactory.getLogger(ConsumerDemoAssignSeek::class.java.name)
        log.info("I am a Kafka Consumer assinged to seek")
        val bootstrapServers = "localhost:19092"
        val topic = "demo_topic"

        // create consumer configs
        val props = Properties()
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        // props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId) // We no longer want to use a group id
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        // create consumer
        val consumer: KafkaConsumer<String, String> = KafkaConsumer(props)

        val partitionToReadFrom = TopicPartition(topic, 0)
        val offsetToReadFrom = 7L
        consumer.assign(listOf(partitionToReadFrom))
        consumer.seek(partitionToReadFrom, offsetToReadFrom)

        val numberOfMessagesToRead = 5
        var keepOnReading = true
        var numberOfMessagesReadSoFar = 0

        // poll for new data
        while (keepOnReading) {
            val records: ConsumerRecords<String, String> = consumer.poll(100)
            for (record in records) {
                numberOfMessagesReadSoFar += 1
                log.info("Key: " + record.key() + ", Value: " + record.value())
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset())
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false // to exit the while loop
                    break // to exit the for loop
                }
            }
        }
        log.info("Read all $numberOfMessagesToRead record from offset $offsetToReadFrom Exiting the application")
    }
}
