package com.nordstrom.kafka.kcr.commands

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.float
import com.nordstrom.kafka.kcr.cassette.CassetteInfo
import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import com.nordstrom.kafka.kcr.metrics.JmxConfigPlay
import com.nordstrom.kafka.kcr.metrics.JmxNameMapper
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.jmx.JmxMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.LoggerFactory
import sun.misc.Signal
import sun.misc.SignalHandler
import java.io.File
import java.security.InvalidParameterException
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.atomic.AtomicLong

class Play : CliktCommand(name = "play", help = "Playback a cassette to a Kafka topic.") {
    private val log = LoggerFactory.getLogger(javaClass)

    // Play options
    private val cassette by option(help = "Kafka Cassette Recorder directory for playback (REQUIRED)")
        .required()
        .validate {
            require(!it.isEmpty()) { "--cassette value cannot be blank" }
            require(!File(it).list().isNullOrEmpty()) { "--cassette $it is empty or invalid" }
        }
    //NB: This initial version can only playback at the capture rate.
    private val playbackRate: Float by option(help = "Playback rate multiplier (1.0 = play at capture rate, 2.0 = playback at twice capture rate)").float()
        .default(1.0f)

    private val topic by option(help = "Kafka topic to write (REQUIRED)")
        .required()
        .validate {
            require(!it.isEmpty()) { "'topic' value cannot be empty or null" }
        }

    private val info by option(help = "List information about a Cassette, then exit").flag()
    private val pause by option(help = "Pause at end of playback (ctrl-c to exit)").flag()

    // Global options from parent command.
    private val opts by requireObject<Properties>()

    private val registry = CompositeMeterRegistry()
    private val start = Date().toInstant()

    private val metricElapsedMillis: AtomicLong?

    init {
        registry.add(JmxMeterRegistry(JmxConfigPlay(), Clock.SYSTEM, JmxNameMapper()))
        metricElapsedMillis = registry.gauge("elapsed-ms", AtomicLong(0))
    }

    //
    // entry
    //
    override fun run() {
        //TODO show()
        println("kcr.play.id: ${opts["kcr.id"]}")
        println("kcr.play.cassette: $cassette")
        println("kcr.play.topic: $topic")
        println("kcr.play.playback-rate: $playbackRate")

        val metricDurationTimer = Timer.start()
        val id = opts["kcr.id"]

        // Remove non-kakfa properties
        val cleanOpts = Properties()
        cleanOpts.putAll(opts)
        cleanOpts.remove("kcr.id")

        // Describe topic to get number of partitions to record.
//        //TODO map recorded partitions to target
//        val admin = KafkaAdminClient(cleanOpts)
//        val numberPartitions = admin.numberPartitions(topic)

        // Read records from each file in cassette to determine timestamp of earliest and latest record.
        // We need this to determine the correct playback sequence of the records since the topic
        // records were written by partition.
        val cinfo = CassetteInfo(cassette)
        if (info) {
            println("kcr.play.info\n${cinfo.summary()}")
            return
        }

        // This is the playback offset (from earliest record to now) that is added to each record's timestamp
        // to determine correct playback time.
        val offsetNanos = ChronoUnit.NANOS.between(cinfo.earliest, start)

        val producerConfig = Properties()
        producerConfig.putAll(cleanOpts)
        producerConfig["key.serializer"] = ByteArraySerializer::class.java.canonicalName
        producerConfig["value.serializer"] = ByteArraySerializer::class.java.canonicalName
        producerConfig["client.id"] = "kcr-$topic-cid-$id]}"
        val client = KafkaProducer<ByteArray, ByteArray>(producerConfig)

        val filelist = File(cassette).list()
        runBlocking {
            for (fileName in filelist) {
                // Skip manifest file
                if (("manifest" in fileName).not()) {
                    log.trace(".run:file=${fileName}")
                    val records = recordsProducer(fileName)
                    // Play records as separate jobs
                    launch {
                        records.consumeEach { record ->
                            play(client, record, offsetNanos)
                        }
                    }
                }
            }
        }
        println("kcr.play.runtime: ${Duration.between(start, Date().toInstant())}")
        metricDurationTimer.stop(registry.timer("duration-ms"))

        if (pause) {
            // Handle ctrl-c
            Signal.handle(Signal("INT"), object : SignalHandler {
                override fun handle(sig: Signal) {
                    println("\nkcr.record.runtime: ${Duration.between(start, Date().toInstant())}")
                    System.exit(0)
                }
            })
            while (true) {
                Thread.sleep(500L)
            }
        }

    }

    // Produces CassetteRecords by reading the partition file.
    fun CoroutineScope.recordsProducer(fileName: String): ReceiveChannel<CassetteRecord> = produce {
        val partitionNumber = fileName.substringAfterLast("-")
        val metricDurationTimer = Timer.start()
        val metricSend = registry.counter("send.total", "partition", partitionNumber)
        val metricSendTotal = registry.counter("send.total")

        val reader = File(cassette, fileName).bufferedReader()
        reader.useLines { lines ->
            lines.forEach { line ->
                // Convert to CassetteRecord.
                // We use json format for now, but will need to write/read bytes to accommodate
                // any kind of payload in the topic.
                @UseExperimental(kotlinx.serialization.UnstableDefault::class)
                val record = Json.parse(CassetteRecord.serializer(), line)
                //TODO adjust timestamp to control playback rate?
                send(record)
                metricSend.increment()
                metricSendTotal.increment()
                updateElapsed()
            }
        }
        metricDurationTimer.stop(
            registry.timer(
                "duration-ms",
                "partition", partitionNumber
            )
        )
    }

    // Plays a record (writes to target Kafka topic)
    private suspend fun play(client: KafkaProducer<ByteArray, ByteArray>, record: CassetteRecord, offsetNanos: Long) {
        val ts = Date(record.timestamp).toInstant()
        val now = Date().toInstant()
        val whenToSend = ts.plusNanos(offsetNanos)
        val wait = Duration.between(now, whenToSend)

        var millis = when (playbackRate > 0.0) {
            true -> (wait.toMillis().toFloat() / playbackRate).toLong()
            //NB: playbackRate == 0, records will not be written in capture order.
            false -> 0L
        }

        //NB: millisecond resolution!
        delay(millis)
        //TODO map record.partition to target topic partition in round-robin fashion.
        val producerRecord = ProducerRecord<ByteArray, ByteArray>(
            topic,
            record.partition,
            record.key?.toByteArray(),
            record.value.toByteArray()
        )
        for (header in record.headers) {
            producerRecord.headers().add(header.key, header.value.toByteArray())
        }
        val future = client.send(producerRecord)
        future.get()
    }

    private fun updateElapsed() {
        metricElapsedMillis?.set(Duration.between(start, Date().toInstant()).toMillis())

    }

    //TODO
//    private fun mapPartition(partition: Int, numberPartitions: Int): Int {
//        return partition % numberPartitions
//    }

}
