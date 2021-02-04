package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.Kcr
import com.nordstrom.kafka.kcr.cassette.Cassette
import com.nordstrom.kafka.kcr.cassette.CassetteInfo
import com.nordstrom.kafka.kcr.commands.options.ConsumerConfigOption
import com.nordstrom.kafka.kcr.commands.options.DurationOption
import com.nordstrom.kafka.kcr.io.FileSinkFactory
import com.nordstrom.kafka.kcr.kafka.KafkaAdminClient
import com.nordstrom.kafka.kcr.kafka.KafkaSourceFactory
import com.nordstrom.kafka.kcr.metrics.JmxConfigRecord
import com.nordstrom.kafka.kcr.metrics.JmxNameMapper
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.jmx.JmxMeterRegistry
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import picocli.CommandLine
import sun.misc.Signal
import sun.misc.SignalHandler
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicLong

@CommandLine.Command(
    name = "record",
    description = ["Record a Kafka topic to a cassette."],
    mixinStandardHelpOptions = true,
    helpCommand = true,
)
class RecordCommand : Runnable {
    @CommandLine.Spec
    lateinit var spec: CommandLine.Model.CommandSpec // injected by picocli

    @CommandLine.ParentCommand
    lateinit var parent: Kcr

    @CommandLine.Mixin
    lateinit var duration: DurationOption

    @CommandLine.Mixin
    lateinit var consumerConfig: ConsumerConfigOption

    @CommandLine.Option(
        names = ["--data-directory"],
        defaultValue = "kcr",
        description = ["Kafka Cassette Recorder data directory for recording (default=kcr)"],
        required = true,
    )
    lateinit var dataDirectory: String

    @CommandLine.Option(
        names = ["--topic"],
        description = ["Kafka topic to record"],
        required = true,
    )
    lateinit var topic: String

    @CommandLine.Option(
        names = ["--group-id"],
        description = ["Kafka consumer group id (default=kcr-<topic>-gid)"],
    )
    var groupId: String = ""

    @CommandLine.Option(
        names = ["--timestamp-header-name"],
        description = ["Kafka message header parameter to extract and use as the record timestamp in epoch format, ignoring record timestamp"],
    )
    var timestampHeaderName: String = ""

    val registry = CompositeMeterRegistry()
    private val start = Date().toInstant()

    init {
        registry.add(JmxMeterRegistry(JmxConfigRecord(), Clock.SYSTEM, JmxNameMapper()))
        //TODO Add common tag 'id'
    }

    override fun run() {
        println("kcr.record.id              : ${Kcr.id}")
        println("kcr.record.topic           : $topic")
        val metricDurationTimer = Timer.start()

        // Add/overwrite consumer config from optional properties file.
        val consumerProperties = parent.kafkaOptions.consumerProperties()
        consumerProperties.putAll(consumerConfig.properties)

        // Describe topic to get number of partitions to record.
        val admin = KafkaAdminClient(consumerProperties)
        val numberPartitions = admin.numberPartitions(topic)
        println("kcr.record.topic.partitions: $numberPartitions")
        if (duration.isPresent) {
            println("kcr.record.duration        : $duration")
        }
        println("kcr.header.timestamp       : ${if (timestampHeaderName.isNotBlank()) timestampHeaderName else "<none>"}")

        // Create a cassette and start recording topic messages
        val sinkFactory = FileSinkFactory()
        val sourceFactory = KafkaSourceFactory(
            id = Kcr.id,
            sourceConfig = consumerProperties,
            topic = topic,
            groupId = groupId
        )
        val cassette =
            Cassette(
                topic = topic,
                partitions = numberPartitions,
                sourceFactory = sourceFactory,
                sinkFactory = sinkFactory,
                dataDirectory = dataDirectory
            )
        cassette.create(Kcr.id)

        // Launch a Recorder co-routine for each partition. Each has a source and sink.  A Recorder reads records
        // from the source and writes to the sink.
        runBlocking<Unit> {
            for (partitionNumber in 0 until numberPartitions) {
                val source = cassette.sources[partitionNumber]
                val sink = cassette.sinks[partitionNumber]
                val recorder = Recorder(source, sink, timestampHeaderName)
                runBlocking {
                    GlobalScope.launch(Dispatchers.IO + CoroutineName("kcr-recorder")) {
                        recorder.record(partitionNumber, registry)
                    }
                }
            }

            if (duration.isPresent) {
                try {
                    delay(duration.millis)
                    coroutineContext[Job]?.cancel()
                    throw Exception("coroutine cancellation")
                } catch (e: Exception) {
                    metricDurationTimer.stop(registry.timer("duration-ms"))
                    val info = CassetteInfo(cassette.cassetteDir)
                    println(info.summary())
                    System.exit(0)
                }
            }

        }


        // Handle ctrl-c
        L.trace(".run.wait-for-sigint")
        Signal.handle(Signal("INT"), object : SignalHandler {
            override fun handle(sig: Signal) {
                metricDurationTimer.stop(
                    registry.timer(
                        "duration-ms"
                    )
                )
                val info = CassetteInfo(cassette.cassetteDir)
                println(info.summary())
                System.exit(0)
            }
        })

        // Update elapsed time metric whilst waiting for ctrl-c
        val metricElapsedMillis = registry.gauge("elapsed-ms", AtomicLong(0))
        while (true) {
            metricElapsedMillis?.set(Duration.between(start, Date().toInstant()).toMillis())
            Thread.sleep(500L)
        }
    }


    companion object {
        val L: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }
}