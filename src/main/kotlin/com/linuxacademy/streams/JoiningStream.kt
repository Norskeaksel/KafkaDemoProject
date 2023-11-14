package com.linuxacademy.streams

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.KStream
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toJavaDuration
import org.apache.kafka.streams.Topology

object JoiningStream {
    @JvmStatic
    fun main(args: Array<String>) {
        val props = streamProperties()
        val topology = buildTopology()
        val streams = KafkaStreams(topology, props)
        runStreamWithGracefulShutdown(streams)
    }

    private fun buildTopology(): Topology? {
        val builder = StreamsBuilder()
        val left: KStream<String, String> = builder.stream("left-topic")
        val right: KStream<String, String> = builder.stream("right-topic")

        // Perform an inner join
        val joinWindowSize = 10.toDuration(DurationUnit.SECONDS).toJavaDuration()
        val joinGracePeriod = 5.toDuration(DurationUnit.HOURS).toJavaDuration()

        val leftJoined: KStream<String, String> = left.leftJoin(
            // join, leftJoin or outerJoin
            right,
            { leftValue: String?, rightValue: String? -> "$leftValue-$rightValue" },
            JoinWindows.ofTimeDifferenceAndGrace(joinWindowSize, joinGracePeriod), // ValueJoiner
        )
        leftJoined.to("joined-topic")
        val topology = builder.build()
        println(topology.describe())
        return topology
    }
}
