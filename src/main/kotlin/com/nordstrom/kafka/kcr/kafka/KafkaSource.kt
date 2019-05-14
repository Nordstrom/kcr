package com.nordstrom.kafka.kcr.kafka

import com.nordstrom.kafka.kcr.io.Source
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class KafkaSource(
    config: Properties,
    val topic: String,
    private val partitionNumber: Int
) : Source {
    private val log = LoggerFactory.getLogger(javaClass)

    private val client: KafkaConsumer<String, String>
    private val consumerConfig: Properties = Properties()

    init {
        consumerConfig.putAll(config)
        consumerConfig["key.deserializer"] = STRING_DESERIALIZER
        consumerConfig["value.deserializer"] = STRING_DESERIALIZER
        consumerConfig["enable.auto.commit"] = "true"

        client = KafkaConsumer<String, String>(consumerConfig)

        log.trace(".init.ok")
    }

    override fun readBytes(): ByteArray {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun assign() {
        val partition = TopicPartition(topic, partitionNumber)
        val partitions = listOf(partition)
        client.assign(partitions)
    }

    fun poll(duration: Duration): ConsumerRecords<String, String>? {
        return client.poll(duration)
    }


    //TODO KafkaConstants
    companion object {
        const val BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDesrializer"
        const val BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer"
        const val STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer"
        const val STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
    }
}