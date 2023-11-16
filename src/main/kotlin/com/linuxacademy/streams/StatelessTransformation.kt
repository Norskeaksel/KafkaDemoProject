package com.linuxacademy.streams

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology

object StatelessTransformation {
    @JvmStatic
    fun main(args: Array<String>) {
        val topology = buildTopology()
        val props = streamProperties()
        val streams = KafkaStreams(topology, props)
        runStreamWithGracefulShutdown(streams)
    }

    private fun buildTopology(): Topology {
        TODO()
    }
}
