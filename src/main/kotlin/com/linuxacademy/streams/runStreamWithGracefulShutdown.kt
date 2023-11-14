package com.linuxacademy.streams

import org.apache.kafka.streams.KafkaStreams
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

fun runStreamWithGracefulShutdown(streams: KafkaStreams) {
    val latch = CountDownLatch(1)

    // Attach a shutdown handler to catch control-c and terminate the application gracefully.
    Runtime.getRuntime().addShutdownHook(object : Thread("streams-shutdown-hook") {
        override fun run() {
            streams.close()
            latch.countDown()
        }
    })
    try {
        streams.start()
        latch.await()
    } catch (e: Throwable) {
        println(e.message)
        exitProcess(1)
    }
    exitProcess(0)
}
