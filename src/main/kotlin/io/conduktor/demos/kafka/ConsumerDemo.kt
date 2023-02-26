package io.conduktor.demos.kafka

import ProducerDemo
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

object ConsumerDemo {
    private val log: Logger = LoggerFactory.getLogger(ProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("I am a Kafka Consumer")

        val bootstrapServers = "localhost:19092"
        val groupId = "consumer_demo" // This must be changed each run to consume from the beginning
        val topic = "demo_topic"

        // create consumer configs
        val props = Properties()
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        // create consumer
        val consumer: KafkaConsumer<String, String> = KafkaConsumer(props)
        consumer.subscribe(listOf(topic))

        // poll for new data
        while (true) {
            val records = consumer.poll(100L)

            for (record in records) {
                log.info("Key: " + record.key() + ", Value: " + record.value())
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset())
            }
        }
    }
}
