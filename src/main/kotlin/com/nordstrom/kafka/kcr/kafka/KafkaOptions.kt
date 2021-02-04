package com.nordstrom.kafka.kcr.kafka

import picocli.CommandLine
import java.util.*

class KafkaOptions {
    @CommandLine.Option(
        names = ["--bootstrap-servers"],
        defaultValue = "\${env:KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}",
        description = ["Kafka bootstrap server list; default: \${DEFAULT-VALUE}"]
    )
    var bootstrapServers: String = ""

    @CommandLine.Option(
        names = ["--security-protocol"],
        defaultValue = "PLAINTEXT", description = ["Security protocol; default: \${DEFAULT-VALUE}"]
    )
    var securityProtocol: String = ""

    @CommandLine.Option(
        names = ["--sasl-mechanism"],
        defaultValue = "SCRAM-SHA-512", description = ["SASL mechanism; default: \${DEFAULT-VALUE}"]
    )
    var saslMechanism: String? = null

    @CommandLine.Option(names = ["--sasl-username"], description = ["SASL username"])
    var saslUsername: String? = System.getenv("KAFKA_SASL_USERNAME")

    @CommandLine.Option(names = ["--sasl-password"], description = ["SASL password"])
    var saslPassword: String? = System.getenv("KAFKA_SASL_PASSWORD")

    fun toMap(): MutableMap<String, String> {
        val propsMap = mutableMapOf(
            "bootstrap.servers" to bootstrapServers,
            "security.protocol" to securityProtocol,
        )

        saslMechanism?.let {
            propsMap["sasl.mechanism"] = saslMechanism!!
        }
        saslUsername?.let {
            propsMap["sasl.jaas.config"] =
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${saslUsername}\" password=\"${saslPassword}\";"
        }

        return propsMap
    }

    // Default kafka producer properties
    fun producerProperties(): Properties {
        val kafkaMap = toMap()
        kafkaMap["client.id"] = "kcr-client-0"
        kafkaMap["key.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"
        kafkaMap["value.serializer"] = "org.apache.kafka.common.serialization.ByteArraySerializer"

        return kafkaMap.toProperties()
    }

    // Default kafka consumer properties
    fun consumerProperties(): Properties {
        val kafkaMap = toMap()
        kafkaMap["key.deserializer"] = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        kafkaMap["value.deserializer"] =
            "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        kafkaMap["enable.auto.commit"] = "true"
        kafkaMap["auto.offset.reset"] = "latest"

        return kafkaMap.toProperties()
    }

    // Default kafka admin properties
    fun adminProperties(): Properties {
        val kafkaMap = toMap()

        return kafkaMap.toProperties()
    }
}