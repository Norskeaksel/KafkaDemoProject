package io.conduktor.demos.kafka

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ConsumerDemo {
    private val log: Logger = LoggerFactory.getLogger(SimpleProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val consumer = setupConsumer()
        log.info("I am a Kafka Consumer")

        while (true) {
            val records = consumer.poll(100L)
            for (record in records) {
                log.info("Key: " + record.key() + ", Value: " + record.value())
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset())
            }
        }
    }
}
