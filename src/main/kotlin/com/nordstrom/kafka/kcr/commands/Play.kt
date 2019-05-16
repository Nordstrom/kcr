package com.nordstrom.kafka.kcr.commands

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.types.float
import org.slf4j.LoggerFactory
import java.util.*

class Play : CliktCommand(name = "play", help = "Playback a cassette to a Kafka topic.") {
    private val log = LoggerFactory.getLogger(javaClass)

    // Play options
    private val cassette by option(help = "Kafka Cassette Recorder directory for playback (REQUIRED)")
            .required()
            .validate {
                require(!it.isEmpty()) { "'cassette' value cannot be empty or null" }
            }
    private val playbackRate: Float? by option(help = "Playback rate in percent").float().default(100.0f)

    private val topic by option(help = "Kafka topic to record (REQUIRED)")
            .required()
            .validate {
                require(!it.isEmpty()) { "'topic' value cannot be empty or null" }
            }

    // Global options from parent command.
    private val opts by requireObject<Map<String, Properties?>>()

    //TODO
    override fun run() {
        log.trace(".run.")

        // For each partition (i.e., file) in the cassette, create a producer.  A supervisor will read all the partition
        // files and coordinate when each record should be written to the topic based on the playbackRate.
    }
}
