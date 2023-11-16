package io.conduktor.demos.kafka

import org.slf4j.LoggerFactory

object SimpleProducerDemo {
    private val log = LoggerFactory.getLogger(SimpleProducerDemo::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val producer = setupProducer()
    }
}
