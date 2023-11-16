package com.linuxacademy.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*

object AggregationsStream {
    @JvmStatic
    fun main(args: Array<String>) {
        val props = streamProperties()
        val topology = buildTopology()
        val streams = KafkaStreams(topology, props)
        runStreamWithGracefulShutdown(streams)
    }

    private fun buildTopology(): Topology? {
        val builder = StreamsBuilder()
        val source: KStream<String, String> = builder.stream("demo_topic")
        val groupedStream: KGroupedStream<String, String> = source.groupByKey()
        // create an aggregation that sums the length in characters of the values for all records sharing the same key
        val aggregatedTable: KTable<String, Int> = groupedStream.aggregate(
            // use .aggregate when doing general aggregations
            { 0 }, // initial value of the aggregation
            { aggKey: String?, newValue: String, aggValue: Int -> aggValue + newValue.length }, // Next aggregate is old aggregate + new value
            Materialized.with(
                // Specify the Serdes for the state store. Needed when different from our topic Serdes
                Serdes.String(),
                Serdes.Integer(),
            ),
        )
        aggregatedTable.toStream().to(
            // Convert the KTable to a KStream before writing to the output topic
            "aggregations-from-demo_topic",
            Produced.with(Serdes.String(), Serdes.Integer()),
        )

        // Count the number of records for each key
        val countedTable: KTable<String, Long> = groupedStream.count(
            Materialized.with(Serdes.String(), Serdes.Long()), // Serdes for the state store
        )
        countedTable.toStream().to(
            // Again convert to KStream before writing to the output topic
            "counts-from-demo-topic",
            Produced.with(Serdes.String(), Serdes.Long()),
        )
        val reducedTable: KTable<String, String> = groupedStream.reduce { aggValue: String, newValue: String ->
            "$aggValue $newValue" // New aggregate is old aggregate concatenated with new value
            // State store are initialized automatically, and it always shares the same Serdes as the input topic.
        }
        reducedTable.toStream()
            .to("concatenated-from-demo-topic") // No need for produced.with() because we are using the same Serdes as the input topic
        val topology = builder.build()
        println(topology.describe())
        return topology
    }
}
