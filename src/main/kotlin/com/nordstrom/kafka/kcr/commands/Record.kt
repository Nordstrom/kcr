package com.nordstrom.kafka.kcr.commands

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.*
import com.nordstrom.kafka.kcr.Kcr.Companion.id
import com.nordstrom.kafka.kcr.cassette.Cassette
import com.nordstrom.kafka.kcr.cassette.CassetteInfo
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
import org.slf4j.LoggerFactory
import sun.misc.Signal
import sun.misc.SignalHandler
import java.io.FileInputStream
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicLong

class Record : CliktCommand(name = "record", help = "Record a Kafka topic to a cassette.") {
    private val log = LoggerFactory.getLogger(javaClass)

    // Record options
    private val dataDirectory by option(help = "Kafka Cassette Recorder data directory for recording (default=$DEFAULT_CASSETTE_DIR)")
        .default(DEFAULT_CASSETTE_DIR)

    private val groupId by option(help = "Kafka consumer group id (default=kcr-<topic>-gid)")
        .validate {
            require(it.isNotEmpty()) { "'group-id' value cannot be empty or null" }
        }

    private val topic by option(help = "Kafka topic to record (REQUIRED)")
        .required()
        .validate {
            require(it.isNotEmpty()) { "'topic' value cannot be empty or null" }
        }
    private val duration by option(help = "Kafka duration for recording, format must be like **h**m**s")
        .validate {
            require(it.isNotEmpty()) { "'duration' value cannot be empty or null" }
            require(Regex("""(\d.*)h(\d.*)m(\d.*)s""").matches(input = it)) {
                "Duration must be in the format of **h**m**s, '**' must be integer or decimal. Please try again!"
            }
        }
    private var hasDuration = false
    private val headerTimestamp by option(help = "Use timestamp from header parameter ignoring record timestamp")
        .default("")
    private val consumerConfig by option(help = "Optional Kafka Consumer configuration file. OVERWRITES any command-line values.")

    // Global options from parent command.
    private val opts by requireObject<Properties>()
    
    val registry = CompositeMeterRegistry()
    private val start = Date().toInstant()

    init {
        registry.add(JmxMeterRegistry(JmxConfigRecord(), Clock.SYSTEM, JmxNameMapper()))
        //TODO Add common tag 'id'
    }


    //
    // entry
    //
    override fun run() {
        hasDuration = duration.isNullOrEmpty().not()
        println("kcr.record.id              : ${opts["kcr.id"]}")
        println("kcr.record.topic           : $topic")
        val metricDurationTimer = Timer.start()

        // Remove non-kakfa properties
        val cleanOpts = Properties()
        cleanOpts.putAll(opts)
        cleanOpts.remove("kcr.id")

        // Add/overwrite consumer config from optional properties file.
        val consumerOpts = Properties()
        if (consumerConfig.isNullOrEmpty().not()) {
            val insConsumerConfig = FileInputStream(consumerConfig)
            consumerOpts.load(insConsumerConfig)
            cleanOpts.putAll(consumerOpts)
        }

        // Describe topic to get number of partitions to record.
        val admin = KafkaAdminClient(cleanOpts)
        val numberPartitions = admin.numberPartitions(topic)
        println("kcr.record.topic.partitions: $numberPartitions")
        println("kcr.record.duration        : $duration")
        println("kcr.header.timestamp       : $headerTimestamp")

        // Create a cassette and start recording topic messages
        val sinkFactory = FileSinkFactory()
        val sourceFactory = KafkaSourceFactory(id = id, sourceConfig = cleanOpts, topic = topic, groupId = groupId)
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
        runBlocking<Unit>{
            for (partitionNumber in 0 until numberPartitions) {
                val source = cassette.sources[partitionNumber]
                val sink = cassette.sinks[partitionNumber]
                val recorder = Recorder(source, sink, headerTimestamp)
                runBlocking {
                    GlobalScope.launch(Dispatchers.IO + CoroutineName("kcr-recorder")) {
                        recorder.record(partitionNumber, registry)
                    }
                }
            }

            if(hasDuration){
                try{
                    var parts = duration!!.split("h", "m", "s")
                    var numDuration: Long = (parts[0].toDouble() * 3600000 + parts[1].toDouble() * 60000 + parts[2].toDouble() * 1000).toLong()
                    delay(numDuration)
                    coroutineContext[Job]?.cancel()
                    throw Exception("coroutine cancellation")
                } catch(e: Exception){
                    metricDurationTimer.stop(registry.timer("duration-ms"))
                    val info = CassetteInfo(cassette.cassetteDir)
                    println(info.summary())
                    System.exit(0)
                }
            }

        }


        // Handle ctrl-c
        log.trace(".run.wait-for-sigint")
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
        val DEFAULT_CASSETTE_DIR = "kcr"
    }

}
