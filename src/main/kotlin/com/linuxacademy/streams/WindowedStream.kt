package com.linuxacademy.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*

object WindowedStream {
    @JvmStatic
    fun main(args: Array<String>) {
        val props = streamProperties()
        val builder = StreamsBuilder()
        val source: KStream<String, String> = builder.stream("demo_topic")
        val groupedStream: KGroupedStream<String, String> = source.groupByKey()
        // Make a hopping window of 10 seconds with 5 second overlaps.
        val windowedStream: TimeWindowedKStream<String, String> =
            groupedStream.windowedBy(TimeWindows.ofSizeWithNoGrace(10.seconds()).advanceBy(5.seconds()))
        // had the advance time been 10 seconds, the windowing strategy would have been a tumbling window.

        val reducedTable: KTable<Windowed<String>, String> =
            windowedStream.reduce { aggValue: String, newValue: String ->
                "$aggValue $newValue"
            }
        reducedTable.toStream().to(
            "windowed-topic",
            // Because we hava a Windowed key we need to specify a Serde when producing.
            Produced.with(WindowedSerdes.sessionWindowedSerdeFrom(String::class.java), Serdes.String()),
        )
        val topology = builder.build()
        val streams = KafkaStreams(topology, props)
        println(topology.describe())
        runStream(streams)
    }
}
