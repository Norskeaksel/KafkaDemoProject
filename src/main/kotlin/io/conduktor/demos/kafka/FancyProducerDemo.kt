package io.conduktor.demos.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

object FancyProducerDemo {
    private val log = LoggerFactory.getLogger(SimpleProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val producer = setupProducer()
        log.info("I am a Kafka Producer")

        repeat(10) {
            // create a producer record
            val topic = "demo_topic"
            val value = "hello world $it"

            // TODO also use key
            val producerRecord = ProducerRecord<String, String>(topic, value)

            // send data - asynchronous
            producer.send(producerRecord) // fire and forget
            // TODO replace with sender that gets metadata

            // Sleep to produce to different partitions
            try {
                Thread.sleep(1000)
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
        // flush data - synchronous
        producer.flush()
        // flush and close producer
        producer.close()
    }
}
