package io.conduktor.demos.kafka

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ConsumerDemo {
    private val log: Logger = LoggerFactory.getLogger(SimpleProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val consumer = setupConsumer()
        // TODO: get records and log them
    }
}
