package com.nordstrom.kafka.kcr.commands

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.validate
import com.nordstrom.kafka.kcr.cassette.Cassette
import com.nordstrom.kafka.kcr.io.FileSinkFactory
import com.nordstrom.kafka.kcr.kafka.KafkaAdminClient
import com.nordstrom.kafka.kcr.kafka.KafkaSourceFactory
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import sun.misc.Signal
import sun.misc.SignalHandler
import java.util.*

class Record : CliktCommand(name = "record", help = "Record a Kafka topic to a cassette.") {
    private val log = LoggerFactory.getLogger(javaClass)

    // Record options
    private val dataDirectory by option(help = "Kafka Cassette Recorder data directory for recording (default=./data)")
        .default("./data")

    private val groupId by option(help = "Kafka consumer group id (default=kcr-<topic>-gid")
        .validate {
            require(it.isNotEmpty()) { "'group-id' value cannot be empty or null" }
        }

    private val topic by option(help = "Kafka topic to record (REQUIRED)")
        .required()
        .validate {
            require(it.isNotEmpty()) { "'topic' value cannot be empty or null" }
        }

    // Global options from parent command.
    private val opts by requireObject<Properties>()


    override fun run() {
        log.trace(".run")
        // Remove non-kakfa properties
        val cleanOpts = Properties()
        cleanOpts.putAll(opts)
        cleanOpts.remove("kcr.id")

        // Describe topic to get number of partitions to record.
        val admin = KafkaAdminClient(cleanOpts)
        val numberPartitions = admin.numberPartitions(topic)

        // Create a cassette and start recording topic messages
        val sinkFactory = FileSinkFactory()
        val sourceFactory = KafkaSourceFactory(cleanOpts, topic, groupId)
        val cassette =
            Cassette(
                topic = topic,
                partitions = numberPartitions,
                sourceFactory = sourceFactory,
                sinkFactory = sinkFactory,
                dataDirectory = dataDirectory
            )
        cassette.create("${opts["kcr.id"]}")

        // Launch a Recorder co-routine for each partition. Each has a source and sink.  A Recorder reads records
        // from the source and writes to the sink.
        for (partitionNumber in 0 until numberPartitions) {
            val source = cassette.sources[partitionNumber]
            val sink = cassette.sinks[partitionNumber]
            val recorder = Recorder(source, sink)
            GlobalScope.launch {
                recorder.record()
            }
        }

        // Handle ctrl-c
        val startTime = System.currentTimeMillis()
        Signal.handle(Signal("INT"), object : SignalHandler {
            override fun handle(sig: Signal) {
                val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                println("\nruntime $elapsed seconds")
                System.exit(0)
            }
        })
        while (true) {
            Thread.sleep(500L)
        }

        log.trace(".run.ok")
    }

}
