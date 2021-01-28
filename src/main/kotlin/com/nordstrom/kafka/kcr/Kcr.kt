package com.nordstrom.kafka.kcr

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.findObject
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.nordstrom.kafka.kcr.cassette.CassetteVersion
import com.nordstrom.kafka.kcr.commands.Play
import com.nordstrom.kafka.kcr.commands.Record
import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import org.slf4j.LoggerFactory
import java.util.*


class KcrVersion {
    //TODO derive from gradle build.
    companion object {
        const val VERSION = "2.0"
    }
}

class Kcr : CliktCommand(
    name = "kcr",
    help = "Apache Kafka topic record/playback tool",
    epilog = "v${KcrVersion.VERSION}/${CassetteVersion.VERSION}"
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val bootstrapServers by option(help = "Kafka bootstrap server list").default("localhost:9092")
    private val securityProtocol by option(help = "Security protocol").default("PLAINTEXT")
    private val saslMechanism by option(help = "SASL mechanism")
    private val saslUsername by option(help = "SASL username").default("")
    private val saslPassword by option(help = "SASL password").default("")

    private val opts by findObject { Properties() }

    override fun run() {
        opts["kcr.id"] = id
        opts["bootstrap.servers"] = bootstrapServers
        opts["security.protocol"] = securityProtocol

        if (securityProtocol.startsWith("SASL")) {
            saslMechanism?.let {
                opts["sasl.mechanism"] = saslMechanism
            }

            opts["sasl.jaas.config"] = when (saslMechanism) {
                "PLAIN" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${saslUsername}\" password=\"${saslPassword}\";"
                "SCRAM-SHA-256", "SCRAM-SHA-512" -> "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${saslUsername}\" password=\"${saslPassword}\";"
                else -> "" // Not supported
            }
        }
    }


    companion object {
        // Make run-id available to everyone.
        val id = AlphaNumKeyGenerator().key(8)
    }

}


//
// KcrKt - main
//
fun main(args: Array<String>) = Kcr()
    .subcommands(Play(), Record())
    .main(args)
