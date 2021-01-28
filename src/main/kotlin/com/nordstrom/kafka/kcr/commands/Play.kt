package com.nordstrom.kafka.kcr.commands

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.float
import com.nordstrom.kafka.kcr.cassette.CassetteInfo
import com.nordstrom.kafka.kcr.cassette.CassetteRecord
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
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.LoggerFactory
import sun.misc.Signal
import sun.misc.SignalHandler
import java.io.File
import java.io.FileInputStream
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
            require(it.isNotEmpty()) { "--cassette value cannot be blank" }
            require(!File(it).list().isNullOrEmpty()) { "--cassette $it is empty or invalid" }
        }
    //NB: This initial version can only playback at the capture rate.
    private val playbackRate: Float by option(help = "Playback rate multiplier (0 = playback as fast as possible, 1.0 = play at capture rate, 2.0 = playback at twice capture rate)").float()
        .default(1.0f)

    private val topic by option(help = "Kafka topic to write (REQUIRED)")
        .required()
        .validate {
            require(it.isNotEmpty()) { "'topic' value cannot be empty or null" }
        }
    private val producerConfig by option(help = "Optional Kafka Producer configuration file. OVERWRITES any command-line values.")

    private val info by option(help = "List information about a Cassette, then exit").flag()
    private val pause by option(help = "Pause at end of playback (ctrl-c to exit)").flag()

    private val numberOfRuns by option(help = "Number of times to run the playback")
    

    private val duration by option(help = "Kafka duration for playback, format must be like **h**m**s")
        .validate {
            require(it.isNotEmpty()) { "'duration' value cannot be empty or null" }
            require(Regex("""(\d.*)h(\d.*)m(\d.*)s""").matches(input = it)) {
                "Duration must be in the format of **h**m**s, '**' must be integer or decimal. Please try again!"
            }
        }
    // Global options from parent command.
    private val opts by requireObject<Properties>()
    private var hasNumOfRuns = false
    private var hasDuration = false

    private val registry = CompositeMeterRegistry()
    private val start = Date().toInstant()

    private var numberPartitions = 0
    private val metricElapsedMillis: AtomicLong?

    init {
        registry.add(JmxMeterRegistry(JmxConfigPlay(), Clock.SYSTEM, JmxNameMapper()))
        metricElapsedMillis = registry.gauge("elapsed-ms", AtomicLong(0))
    }

    //
    // entry
    //
    override fun run() {

        hasNumOfRuns = numberOfRuns.isNullOrEmpty().not()
        hasDuration = duration.isNullOrEmpty().not()

        if( hasNumOfRuns && hasDuration ){
            println("Error: option --number-of-runs cannot be used with --duration")
            System.exit(0)
        }

        //TODO show()
        println("kcr.play.id      : ${opts["kcr.id"]}")
        println("kcr.play.topic   : $topic")
        println("kcr.play.playback-rate: $playbackRate")

        val metricDurationTimer = Timer.start()
        val id = opts["kcr.id"]

        // Remove non-kakfa properties
        val cleanOpts = Properties()
        cleanOpts.putAll(opts)
        cleanOpts.remove("kcr.id")

        // Describe topic to get number of partitions of playback topic.
        val admin = KafkaAdminClient(cleanOpts)
        numberPartitions = admin.numberPartitions(topic)

        // Read records from each file in cassette to determine timestamp of earliest and latest record.
        // We need this to determine the correct playback sequence of the records since the topic
        // records were written by partition.
        val cinfo = CassetteInfo(cassette)
        println(cinfo.summary())
        if (info) {
            return
        }
        if (cinfo.totalRecords <= 0) {
            println("No records to play")
            return
        }
        // Handle ctrl-c
        Signal.handle(Signal("INT"), object : SignalHandler {
            override fun handle(sig: Signal) {
                println(".exit.")
                //TODO print metrics summary
                System.exit(0)
            }
        })

        // Add/overwrite producer config from optional properties file.
        val producerOpts = Properties()
        producerOpts["key.serializer"] = ByteArraySerializer::class.java.canonicalName
        producerOpts["value.serializer"] = ByteArraySerializer::class.java.canonicalName
        producerOpts["client.id"] = "kcr-$topic-cid-$id]}"
        if (producerConfig.isNullOrEmpty().not()) {
            val insProducerConfig = FileInputStream(producerConfig)
            producerOpts.load(insProducerConfig)
            cleanOpts.putAll(producerOpts)
        }
        producerOpts.putAll(cleanOpts)
        val client = KafkaProducer<ByteArray, ByteArray>(producerOpts)

        val filelist = File(cassette).list()

        val startKcr = Date().toInstant()

        if(hasDuration) {
            runWithDuration(cinfo, client, filelist)
        } else {
            runWithCount(cinfo, client, filelist)
        }

        println("kcr.play.runtime : ${Duration.between(startKcr, Date().toInstant())}")
        metricDurationTimer.stop(registry.timer("duration-ms"))

        if (pause) {
            // Handle ctrl-c
            Signal.handle(Signal("INT"), object : SignalHandler {
                override fun handle(sig: Signal) {
                    System.exit(0)
                }
            })
            while (true) {
                Thread.sleep(500L)
            }
        }

    }

    private fun runWithDuration(cinfo: CassetteInfo, client: KafkaProducer<ByteArray, ByteArray>, filelist: Array<String>) {
        var abort = false
        val parts = duration!!.split("h", "m", "s")
        var timeLeftMillis = (parts[0].toDouble() * 3600000 + parts[1].toDouble() * 60000 + parts[2].toDouble() * 1000).toLong()
        // val startKcr = Date().toInstant()
        while (!abort && shouldContinueWithDuration(timeLeftMillis)) {
            val runStart = Date().toInstant()
            runBlocking {
                try {
                    withTimeout(timeLeftMillis) {
                        // This is the playback offset (from earliest record to now) that is added to each record's timestamp
                        // to determine correct playback time.
                        val offsetNanos = ChronoUnit.NANOS.between(cinfo.earliest, Date().toInstant())
                        for (fileName in filelist) {
                            // Skip manifest file
                            if (("manifest" in fileName).not()) {
                                log.trace(".run:file=${fileName}")
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

    private fun runWithCount(cinfo: CassetteInfo, client: KafkaProducer<ByteArray, ByteArray>, filelist: Array<String>) {
        var iRuns = 0
        while (shouldContinueWithCount(iRuns)) {
            runBlocking {
                // This is the playback offset (from earliest record to now) that is added to each record's timestamp
                // to determine correct playback time.
                val offsetNanos = ChronoUnit.NANOS.between(cinfo.earliest, Date().toInstant())
                for (fileName in filelist) {
                    // Skip manifest file
                    if (("manifest" in fileName).not()) {
                        log.trace(".run:file=${fileName}")
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
    private suspend fun play(client: KafkaProducer<ByteArray, ByteArray>, record: CassetteRecord, offsetNanos: Long) {
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
        val producerRecord = ProducerRecord<ByteArray, ByteArray>(
            topic,
            partitionToUse,
            record.key?.toByteArray(),
            record.value.toByteArray()
        )
        for (header in record.headers) {
            producerRecord.headers().add(header.key, header.value.toByteArray())
        }
        try {
            val future = client.send(producerRecord)
            future.get()
        } catch (e: Exception) {
            println( "ERROR during send: partition=${record.partition}, key=${record.key}, exception=${e}")
        }
    }

    private fun updateElapsed() {
        metricElapsedMillis?.set(Duration.between(start, Date().toInstant()).toMillis())

    }

    private fun shouldContinueWithDuration(timeLeftMillis: Long) : Boolean {
        if( timeLeftMillis > 0 ) {
            return true
        }
        return false
    }

    private fun shouldContinueWithCount(runCount: Int) : Boolean {
        if (!hasNumOfRuns && runCount == 0) {
            return true
        } else if( hasNumOfRuns && (numberOfRuns!!.toInt() == 0) ) {
            return true
        } else if ( hasNumOfRuns && (runCount < numberOfRuns!!.toInt()) ) {
            return true
        }
        return false
    }

    //TODO
    private fun mapPartition(partition: Int, numberPartitions: Int): Int {
        return partition % numberPartitions
    }

}