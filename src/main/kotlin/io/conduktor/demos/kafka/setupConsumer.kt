package io.conduktor.demos.kafka // ktlint-disable filename
import ConfigVariables.BOOTSTRAP_SERVERS
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties

fun setupConsumer(): KafkaConsumer<String, String> {
    // Define essential variables
    val bootstrapServers = BOOTSTRAP_SERVERS
    val groupId = "consumer_demo" // This must be changed each run to consume from the beginning
    val topic = "demo_topic"

    // create 5 consumer configs
    val props = Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Initialize consumer with properties and subscribe to topic
    val consumer = KafkaConsumer<String, String>(props)
    consumer.subscribe(listOf(topic))
    return consumer
}
