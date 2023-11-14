package io.conduktor.demos.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

object ProducerDemo {
    private val log = LoggerFactory.getLogger(ProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val producer = setupProducer()
        log.info("I am a Kafka Producer")

        repeat(10) {
            // create a producer record
            val topic = "demo_topic"
            val value = "hello world $it"
            val key = listOf("key1", "key2", "key3").random()
            val partition = (key.last() - '0') % 3

            // val producerRecord = ProducerRecord<String, String>(topic, value)
            val producerRecord = ProducerRecord(topic, key, value)
            // val producerRecord = ProducerRecord(topic, partition, key, value)

            // send data - asynchronous
            // producer.send(producerRecord) // fire and forget
            producer.send(producerRecord) { recordMetadata, e -> // executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    log.info(
                        """
                    Received new metadata. 
                    Topic:${recordMetadata.topic()}
                    Key:  ${producerRecord.key()}
                    Partition: ${recordMetadata.partition()}
                    Offset: ${recordMetadata.offset()}
                    Timestamp: ${recordMetadata.timestamp()}
                        """.trimIndent(),
                    )
                } else {
                    log.error("Error while producing", e)
                }
            }
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
