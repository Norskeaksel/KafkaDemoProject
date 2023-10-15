package io.conduktor.demos.kafka
import ConfigVariables.BOOTSTRAP_SERVERS
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

fun setupProducer(): KafkaProducer<String, String> {
    val bootstrapServers = BOOTSTRAP_SERVERS
    val props = Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    return KafkaProducer(props)
}
