package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.Kcr
import com.nordstrom.kafka.kcr.cassette.CassetteInfo
import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import com.nordstrom.kafka.kcr.commands.options.DurationOption
import com.nordstrom.kafka.kcr.commands.options.NumberOfPlaysOption
import com.nordstrom.kafka.kcr.commands.options.ProducerConfigOption
import com.nordstrom.kafka.kcr.kafka.KafkaAdminClient
import com.nordstrom.kafka.kcr.metrics.JmxConfigPlay
import com.nordstrom.kafka.kcr.metrics.JmxNameMapper
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.composite.CompositeMeterRegistry
import io.micrometer.jmx.JmxMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.apache.commons.codec.binary.Hex
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import picocli.CommandLine
import sun.misc.Signal
import java.io.File
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.atomic.AtomicLong

@CommandLine.Command(
    name = "play",
    description = ["Playback a cassette to a Kafka topic."],
    mixinStandardHelpOptions = true,
    helpCommand = true,
)
class PlayCommand : Runnable {
    @CommandLine.Spec
    lateinit var spec: CommandLine.Model.CommandSpec

    @CommandLine.ParentCommand
    lateinit var parent: Kcr

    @CommandLine.Mixin
    lateinit var duration: DurationOption

    @CommandLine.Mixin
    lateinit var numberOfPlays: NumberOfPlaysOption

    @CommandLine.Mixin
    lateinit var producerConfig: ProducerConfigOption

    @CommandLine.Option(
        names = ["--cassette"],
        description = ["Kafka Cassette Recorder cassette directory for playback"],
        required = true,
    )
    lateinit var cassette: String

    @CommandLine.Option(
        names = ["--topic"],
        description = ["Kafka topic to write."],
        required = true,
    )
    lateinit var topic: String

    @CommandLine.Option(
        names = ["--info"],
        description = ["List information about a cassette, then exit (default=false)"],
    )
    var info: Boolean = false

    @CommandLine.Option(
        names = ["--pause"],
        description = ["Pause at end of playback; ctrl-c to exit (default=false)"],
    )
    var pause: Boolean = false

    @CommandLine.Option(
        names = ["--playback-rate"],
        description = ["Playback rate multiplier (0 = playback as fast as possible, 1.0 = play at capture rate, 2.0 = playback at twice capture rate, default=1.0)"],
    )
    var playbackRate: Float = 1.0f

    private val registry = CompositeMeterRegistry()
    private val start = Date().toInstant()

    private var numberPartitions = 0
    private val metricElapsedMillis: AtomicLong?

    init {
        registry.add(JmxMeterRegistry(JmxConfigPlay(), Clock.SYSTEM, JmxNameMapper()))
        metricElapsedMillis = registry.gauge("elapsed-ms", AtomicLong(0))
    }

    /*
    Run Play command
     */
    override fun run() {
        if (numberOfPlays.isPresent && duration.isPresent) {
            throw CommandLine.ParameterException(
                spec.commandLine(),
                "--duration and --number-of-plays are mutually exclusive"
            )
        }

        // Read records from each file in cassette to determine timestamp of earliest and latest record.
        // We need this to determine the correct playback sequence of the records since the topic
        // records were written by partition.
        val cinfo = CassetteInfo(cassette)
        println(cinfo.summary())
        if (info) {
            System.exit(0)
        }
        if (cinfo.totalRecords <= 0) {
            println("No records to play")
            return
        }

        println("kcr.play.id              : ${Kcr.id}")
        println("kcr.play.topic           : $topic")
        println("kcr.play.playback-rate   : ${playbackRate}X")
        if (duration.isPresent) {
            println("kcr.play.duration        : ${Duration.ofMillis(duration.millis)}")
        }

        val metricDurationTimer = Timer.start()

        // Describe topic to get number of partitions of playback topic.
        val admin = KafkaAdminClient(parent.kafkaOptions.adminProperties())
        numberPartitions = admin.numberPartitions(topic)
        println("kcr.play.topic.partitions: $numberPartitions")

        // Handle ctrl-c
        Signal.handle(Signal("INT")) {
            println(".exit.")
            //TODO print metrics summary
            System.exit(0)
        }

        // Add/overwrite producer config from optional properties file.
        val producerProperties = parent.kafkaOptions.producerProperties()
        producerProperties["client.id"] = "kcr-$topic-cid-${Kcr.id}"
        producerProperties.putAll(producerConfig.properties)
        val client = KafkaProducer<ByteArray, ByteArray>(producerProperties)

        val filelist = File(cassette).list()

        val startKcr = Date().toInstant()

        if (duration.isPresent) {
            runWithDuration(cinfo, client, filelist)
        } else {
            runWithCount(cinfo, client, filelist)
        }

        println("kcr.play.runtime : ${Duration.between(startKcr, Date().toInstant())}")
        metricDurationTimer.stop(registry.timer("duration-ms"))

        if (pause) {
            // Handle ctrl-c
            Signal.handle(Signal("INT")) { System.exit(0) }
            while (true) {
                Thread.sleep(500L)
            }
        }

    }

    private fun runWithDuration(
        cinfo: CassetteInfo,
        client: KafkaProducer<ByteArray, ByteArray>,
        filelist: Array<String>
    ) {
        var abort = false
        var timeLeftMillis = duration.millis
        // val startKcr = Date().toInstant()
        while (!abort && shouldContinueWithDuration(timeLeftMillis)) {
            val runStart = Date().toInstant()
            runBlocking {
                try {
                    withTimeout(timeLeftMillis) {
                        // This is the playback offset (from earliest record to now) that is added to each record's timestamp
                        // to determine correct playback time.
                        val offsetNanos =
                            ChronoUnit.NANOS.between(cinfo.earliest, Date().toInstant())
                        for (fileName in filelist) {
                            // Skip manifest file
                            if (("manifest" in fileName).not()) {
                                L.trace(".run:file=${fileName}")
                                val records = recordsProducer(fileName)
                                // Play records as separate jobs
                                launch(Dispatchers.IO + CoroutineName("kcr-player")) {
                                    records.consumeEach { record ->
                                        play(client, record, offsetNanos)
                                    }
                                }
                            }
                        }
                    }
                } catch (e: TimeoutCancellationException) {
                    abort = true
                }
            } //-runBlocking

            val dt = Duration.between(runStart, Date().toInstant())
            // println( "runBlocking.OK:" + dt + ", elapsed=" + Duration.between(startKcr, Date().toInstant()) )
            timeLeftMillis -= dt.toMillis()
        }   //-while

    }

    private fun runWithCount(
        cinfo: CassetteInfo,
        client: KafkaProducer<ByteArray, ByteArray>,
        filelist: Array<String>
    ) {
        var iRuns = 0
        while (shouldContinueWithCount(iRuns)) {
            runBlocking {
                // This is the playback offset (from earliest record to now) that is added to each record's timestamp
                // to determine correct playback time.
                val offsetNanos = ChronoUnit.NANOS.between(cinfo.earliest, Date().toInstant())
                for (fileName in filelist) {
                    // Skip manifest file
                    if (("manifest" in fileName).not()) {
                        L.trace(".run:file=${fileName}")
                        val records = recordsProducer(fileName)
                        // Play records as separate jobs
                        launch(Dispatchers.IO + CoroutineName("kcr-player")) {
                            records.consumeEach { record ->
                                play(client, record, offsetNanos)
                            }
                        }
                    }
                }
            }
            iRuns++
            // println( "run number:" + iRuns )
        }

    }

    // Produces CassetteRecords by reading the partition file.
    @OptIn(ExperimentalCoroutinesApi::class)
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
                val record = Json.decodeFromString<CassetteRecord>(line)
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
    private suspend fun play(
        client: KafkaProducer<ByteArray, ByteArray>,
        record: CassetteRecord,
        offsetNanos: Long
    ) {
        val ts = Date(record.timestamp).toInstant()
        val now = Date().toInstant()
        val whenToSend = ts.plusNanos(offsetNanos)
        val wait = Duration.between(now, whenToSend)

        val millis = when (playbackRate > 0.0) {
            true -> (wait.toMillis().toFloat() / playbackRate).toLong()
            //NB: playbackRate == 0, records will not be written in capture order.
            false -> 0L
        }

        //NB: millisecond resolution!
        delay(millis)
        //TODO map record.partition to target topic partition in round-robin fashion.
        val partitionToUse = mapPartition(record.partition, numberPartitions)
        // TODO delegate to plugin for modified record w/headers
        val producerRecord = ProducerRecord<ByteArray, ByteArray>(
            topic,
            partitionToUse,
            record.key?.toByteArray(),
            Hex.decodeHex(record.value)
        )
        for (header in record.headers) {
            producerRecord.headers().add(header.key, header.value.toByteArray())
        }
        try {
            val future = client.send(producerRecord)
            future.get()
        } catch (e: Exception) {
            println("ERROR during send: partition=${record.partition}, key=${record.key}, exception=${e}")
        }
    }

    private fun updateElapsed() {
        metricElapsedMillis?.set(Duration.between(start, Date().toInstant()).toMillis())

    }

    private fun shouldContinueWithDuration(timeLeftMillis: Long): Boolean {
        if (timeLeftMillis > 0) {
            return true
        }
        return false
    }

    private fun shouldContinueWithCount(runCount: Int): Boolean {
        if (!numberOfPlays.isPresent && runCount == 0) {
            return true
        } else if (numberOfPlays.isPresent && (numberOfPlays.count == 0)) {
            return true
        } else if (numberOfPlays.isPresent && (runCount < numberOfPlays.count)) {
            return true
        }
        return false
    }

    //TODO
    private fun mapPartition(partition: Int, numberPartitions: Int): Int {
        return partition % numberPartitions
    }

    companion object {
        val L: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
    }
}