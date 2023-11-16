package com.linuxacademy.streams

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record

object PunctuatedStream {
    @JvmStatic
    fun main(args: Array<String>) {
        val props = streamProperties()
        val builder = StreamsBuilder()
        val source: KStream<String, String> = builder.stream("punctuate")
        source.process(ProcessorSupplier { Punctuator() }).to("punctuate-output")
        val topology = builder.build()
        println(topology.describe())
        val streams = KafkaStreams(topology, props)
        runStreamWithGracefulShutdown(streams)
    }
}

object Previous {
    var record: Record<String, String> = Record("key", "0", 0)
    var needsPunctuation = false
}

class Punctuator : Processor<String, String, String, String> {
    private lateinit var context: ProcessorContext<String, String>

    override fun init(context: ProcessorContext<String, String>) {
        this.context = context
        context.schedule(
            10.seconds(),
            PunctuationType.WALL_CLOCK_TIME,
            ::streamTimePunctuator,
        )
    }

    private fun streamTimePunctuator(timestamp: Long?) {
        if (Previous.needsPunctuation) {
            context.forward(Previous.record)
        }
        Previous.needsPunctuation = true
    }

    override fun process(record: Record<String, String>) {
        val oldInterval = Previous.record.value().toInt() / 10
        val newInterval = record.value().toInt() / 10
        val isNewInterval = newInterval > oldInterval
        val oldIntervalRecord = Previous.record
        if (record.value().toInt() > Previous.record.value().toInt()) {
            Previous.record = record
            Previous.needsPunctuation = false
        }
        if (isNewInterval) {
            context.forward(oldIntervalRecord)
        }
    }
}
