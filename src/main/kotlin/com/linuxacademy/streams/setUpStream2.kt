package com.linuxacademy.streams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import java.util.*

fun setUpStream(): Properties {
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "MyStreamsGroup"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:29092"
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
    props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0) // Disable caching. Good for testing, bad for production.
    return props
}
