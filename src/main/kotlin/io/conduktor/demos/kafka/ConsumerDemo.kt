package io.conduktor.demos.kafka

import ProducerDemo
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.*

object ConsumerDemo {
    private val log: Logger = LoggerFactory.getLogger(ProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("I am a Kafka Consumer")

        val bootstrapServers = "localhost:19092"
        val groupId = "consumer_demo1"
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

        // get a reference to the current thread
        val mainThread = Thread.currentThread()

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...")
                consumer.wakeup()

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join()
                } catch (e: InterruptedException) {
                    e.printStackTrace()
                }
            }
        })
        try {
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
        catch (e:WakeupException) {
            log.info("Wake up exception!")
            // we ignore this as this is an expected exception when closing a consumer
        } catch (e:Exception) {
            log.error("Unexpected exception", e)
        } finally {
            consumer.close() // this will also commit the offsets if need be.
            log.info("The consumer is now gracefully closed.")
        }
    }
}
