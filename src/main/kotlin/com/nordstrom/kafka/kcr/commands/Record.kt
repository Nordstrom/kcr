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
import com.nordstrom.kafka.kcr.kafka.KafkaSource
import com.nordstrom.kafka.kcr.kafka.KafkaSourceFactory
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import sun.misc.Signal
import sun.misc.SignalHandler
import java.time.Duration
import java.util.*
import kotlinx.serialization.*
import kotlinx.serialization.json.Json

class Record : CliktCommand(name = "record", help = "Record a Kafka topic to a cassette.") {
    @Serializable
    class CassetteRecord(
        val headers: MutableMap<String, String> = mutableMapOf<String, String>(),
        val timestamp: Long,
        val partition: Int,
        val offset: Long,
        val key: String?,
        val value: String
    ) {
    }

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
        println(".kcr-record.run.")

        // Describe topic to get number of partitions (tracks) to record.
        val client = KafkaAdminClient(opts)
        val numberPartitions = client.numberPartitions(topic)

        // Create a cassette and start recorder (threads) to begin recording topic messages
        val sinkFactory = FileSinkFactory()
        val sourceFactory = KafkaSourceFactory(opts, topic, groupId)
        val cassette =
            Cassette(
                topic = topic,
                tracks = numberPartitions,
                sourceFactory = sourceFactory,
                sinkFactory = sinkFactory,
                dataDirectory = dataDirectory
            )
        cassette.create()

        for (partitionNumber in 0 until numberPartitions) {
            GlobalScope.launch {
                val source = cassette.sources[partitionNumber]
                val sink = cassette.sinks[partitionNumber]
                if (source is KafkaSource) {
                    source.assign()
                    while (true) {
                        val records = source.poll(Duration.ofSeconds(20))
                        records?.iterator()?.forEach {
                            val record = CassetteRecord(
                                timestamp = it.timestamp(),
                                partition = it.partition(),
                                offset = it.offset(),
                                key = it.key(),
                                value = it.value()
                            )
                            it.headers().forEach { header ->
                                record.headers[header.key()] = String(header.value())
                            }
                            val data = Json.stringify(CassetteRecord.serializer(), record)
                            sink?.writeText("$data\n")
                        }
                    }
                }
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
    }

}
