package io.conduktor.demos.kafka

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ConsumerDemo {
    private val log: Logger = LoggerFactory.getLogger(ProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("I am a Kafka Consumer")

        // create consumer
        val consumer = setUpConsumer()
        val topic = "demo_topic"
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
