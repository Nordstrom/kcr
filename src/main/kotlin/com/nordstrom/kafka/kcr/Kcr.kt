package com.nordstrom.kafka.kcr

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.findObject
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.nordstrom.kafka.kcr.commands.Play
import com.nordstrom.kafka.kcr.commands.Record
import java.util.*


class Kcr : CliktCommand() {
    val bootstrapServers by option(help = "Kafka bootstrap server list").default("localhost:9092")
    val securityProtocol by option(help = "Security protocol").default("SASL_SSL")
    val saslMechanism by option(help = "SASL mechanism").default("SCRAM-SHA-512")
    val saslUsername by option(help = "SASL username").default("")
    val saslPassword by option(help = "SASL password").default("")

    val opts by findObject { Properties() }

    override fun run() {
        echo(".kcr.run")
        // Properties are passed on to the subcomman d.
        opts["bootstrap.servers"] = bootstrapServers
        opts["security.protocol"] = securityProtocol
        opts["sasl.mechanism"] = saslMechanism
        val jaas = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${saslUsername}\" password=\"${saslPassword}\";"
        opts["sasl.jaas.config"] = jaas
    }
}


// KcrKt
fun main(args: Array<String>) = Kcr()
        .subcommands(Play(), Record())
        .main(args)
