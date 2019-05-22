package com.nordstrom.kafka.kcr.kafka

import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.io.SourceFactory
import org.slf4j.LoggerFactory
import java.util.*

class KafkaSourceFactory(
    sourceConfig: Properties,
    private val topic: String,
    groupId: String?,
    private val id: String
) : SourceFactory {
    private val log = LoggerFactory.getLogger(javaClass)

    private val config: Properties = Properties()

    init {
        config.putAll(sourceConfig)
        var gid = groupId
        if (groupId.isNullOrBlank()) {
            gid = "kcr-$topic-gid-$id"
        }
        config["group.id"] = gid
    }

    override fun create(partition: Int): Source {
        // Unique cid
        val cid = "kcr-$topic-cid-$id-$partition"
        config["client.id"] = cid

        return KafkaSource(config, topic, partition)
    }

}