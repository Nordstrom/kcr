package com.nordstrom.kafka.kcr.commands.options

import picocli.CommandLine
import java.io.File
import java.io.FileInputStream
import java.util.*

class DurationOption {
    @CommandLine.Spec
    lateinit var spec: CommandLine.Model.CommandSpec

    @CommandLine.Option(
        names = ["--duration"],
        description = ["Duration for playback; format must be like **h**m**s"],
    )
    fun setDurationValue(value: String) {
        var valid = true
        valid = valid && value.isNotBlank()
        valid = valid && Regex("""(\d.*)h(\d.*)m(\d.*)s""").matches(input = value)
        if (!valid) {
            throw CommandLine.ParameterException(
                spec.commandLine(),
                "--duration must be in the format of **h**m**s, '**' must be integer or decimal. Please try again!"
            )
        }

        val parts = value.split("h", "m", "s")
        millis =
            (parts[0].toDouble() * 3600000 + parts[1].toDouble() * 60000 + parts[2].toDouble() * 1000).toLong()
        isPresent = true
    }

    var millis = 0L
    var isPresent = false
}

class NumberOfPlaysOption {
    @CommandLine.Spec
    lateinit var spec: CommandLine.Model.CommandSpec

    @CommandLine.Option(
        names = ["--number-of-plays"],
        description = ["Number of times to play the cassette"],
    )
    fun setNumberOfPlaysValue(value: Int) {
        if (value <= 0) {
            throw CommandLine.ParameterException(
                spec.commandLine(),
                "--number-of-plays must be greater than 0"
            )
        }
        count = value
        isPresent = true
    }

    var count: Int = 0
    var isPresent = false

}

class ConsumerConfigOption {
    @CommandLine.Spec
    lateinit var spec: CommandLine.Model.CommandSpec

    @CommandLine.Option(
        names = ["--consumer-config"],
        description = ["Optional Kafka Consumer configuration file. OVERWRITES any command-line values."],
    )
    fun setProperties(value: File) {
        if (value.isFile.not()) {
            throw CommandLine.ParameterException(
                spec.commandLine(),
                "File not found: $value"
            )
        }

        properties.load(FileInputStream(value))

        file = value
        isPresent = true
    }

    lateinit var file: File
    var properties = Properties()
    var isPresent = false
}

class ProducerConfigOption {
    @CommandLine.Spec
    lateinit var spec: CommandLine.Model.CommandSpec

    @CommandLine.Option(
        names = ["--producer-config"],
        description = ["Optional Kafka Producer configuration file. OVERWRITES any command-line values."],
    )
    fun setProperties(value: File) {
        if (value.isFile.not()) {
            throw CommandLine.ParameterException(
                spec.commandLine(),
                "File not found: $value"
            )
        }

        properties.load(FileInputStream(value))

        file = value
        isPresent = true
    }

    lateinit var file: File
    var properties = Properties()
    var isPresent = false
}