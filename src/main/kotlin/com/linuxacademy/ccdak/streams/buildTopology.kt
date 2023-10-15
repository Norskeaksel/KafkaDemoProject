package com.linuxacademy.ccdak.streams

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream

fun buildTopology(): Topology {
    val builder = StreamsBuilder()
    val source: KStream<String?, String> = builder.stream("demo_topic")
    val evenPredicate: (String?, String?) -> Boolean = { _, value -> (value?.last()?.code ?: 1) % 2 == 0 && value?.last()?.code != 4 }

    val evenStream = source.filter(evenPredicate)
    val oddStream = source.filterNot(evenPredicate)
    val foursStream = source.filter { _, value -> value.contains('4') }.
        map{ key, value -> KeyValue.pair(key, value.uppercase())}

    oddStream.to("odd-output-topic")
    val refinedEvenStream = evenStream.merge(foursStream)
    refinedEvenStream.to("even-output-topic")

    return builder.build()

}