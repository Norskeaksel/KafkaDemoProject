import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*


object ProducerDemo {
    private val log = LoggerFactory.getLogger(ProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("I am a Kafka Producer")
        val bootstrapServers = "localhost:19092"

        // create Producer properties
        val props = Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        // create the producer
        val producer = KafkaProducer<String, String>(props)

        repeat(10) {
            // create a producer record
            val topic = "demo_topic"
            val value = "hello world " + Integer.toString(it)
            val key = UUID.randomUUID().toString()

            val producerRecord = ProducerRecord<String, String>(topic, value)
            //val producerRecord = ProducerRecord(topic, key, value)

            // send data - asynchronous
            //producer.send(producerRecord) // fire and forget
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
                    """.trimIndent()
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
