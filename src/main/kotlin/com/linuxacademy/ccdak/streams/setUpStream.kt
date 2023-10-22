package com.linuxacademy.ccdak.streams

import ConfigVariables.BOOTSTRAP_SERVERS
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.util.*

fun setUpStream(): KafkaStreams {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "stateless-transformations-example"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.getName()

    val topology = buildTopology()
    println(topology.describe())
    return KafkaStreams(topology, props)
}
