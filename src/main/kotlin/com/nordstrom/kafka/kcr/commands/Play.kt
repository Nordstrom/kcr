package com.nordstrom.kafka.kcr.commands

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.types.float
import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import java.io.BufferedReader
import java.io.File
import java.io.InputStream
import java.nio.charset.Charset
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*

class Play : CliktCommand(name = "play", help = "Playback a cassette to a Kafka topic.") {
    private val log = LoggerFactory.getLogger(javaClass)

    // Play options
    private val cassette by option(help = "Kafka Cassette Recorder directory for playback (REQUIRED)")
        .required()
        .validate {
            require(!it.isEmpty()) { "'cassette' value cannot be empty or null" }
        }
//    private val playbackRate: Float by option(help = "Playback rate multiplier (1.0 = play at capture rate, 2.0 = playback at twice capture rate)").float().default(
//        1.0f
//    )
    //NB: This initial version can only playback at the capture rate.
    private val playbackRate = 1.0f

    private val topic by option(help = "Kafka topic to record (REQUIRED)")
        .required()
        .validate {
            require(!it.isEmpty()) { "'topic' value cannot be empty or null" }
        }

    // Global options from parent command.
    private val opts by requireObject<Map<String, Properties?>>()


    override fun run() {
        log.trace("run")
        val now = Date().toInstant()

        val filelist = File(cassette).list()

        // Read first record from each file in cassette to determine timestamp of earliest record.
        // We need this to determine the correct playback sequence of the records since the topic
        // records were written by partition.
        var earliest: Long = Long.MAX_VALUE
        for (file in filelist) {
            if (("manifest" in file).not()) {
                val line = File(cassette, file).readLines()[0]
                val record = Json.parse(CassetteRecord.serializer(), line)
                if (record.timestamp < earliest) {
                    earliest = record.timestamp
                }
            }
        }
        log.trace("earliest=$earliest, ${Date(earliest)}, ${Date(earliest).toInstant()}")
        val start = Date(earliest).toInstant()
        // This is the playback offset (from earliest record to now) that is added to each record's timestamp
        // to determine correct playback time.
        val offsetNanos = ChronoUnit.NANOS.between(start, now)

        runBlocking {
            for (fileName in filelist) {
                // Skip manifest file
                if (("manifest" in fileName).not()) {
                    val records = recordsProducer(fileName, offsetNanos)
                    // Play records as separate jobs
                    launch {
                        records.consumeEach { record ->
                            play(record, offsetNanos)
                        }
                    }
                }
            }
        }
    }

    // Produces CassetteRecords by reading the partition file.
    fun CoroutineScope.recordsProducer(fileName: String, offsetNanos: Long): ReceiveChannel<CassetteRecord> = produce {
        val reader = File(cassette, fileName).bufferedReader()
        reader.useLines { lines ->
            lines.forEach { line ->
                // Convert to CassetteRecord.
                // We use json format for now, but will need to write/read bytes to accommodate
                // any kind of payload in the topic.
                val record = Json.parse(CassetteRecord.serializer(), line)
                send(record)
            }
        }
    }

    // Plays a record (writes to target Kafka topic)
    private suspend fun play(record: CassetteRecord, offsetNanos: Long) {
        val ts = Date(record.timestamp).toInstant()
        val now = Date().toInstant()
        val whenToSend = ts.plusNanos(offsetNanos)
        val wait = Duration.between(now, whenToSend)

        var millis = when (playbackRate > 0.0) {
            true -> (wait.toMillis().toFloat() / playbackRate).toLong()
            false -> 0
        }

//        log.trace("n:wait=$wait, ${wait.toMillis()}, ${millis} from now=$now")
        //NB: millisecond resolution!
        delay(millis)
        log.trace("play: -> ts=${record.timestamp}, partition:offset=${record.partition}:${record.offset} ${record.value}")
    }

}
