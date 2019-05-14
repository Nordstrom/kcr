package com.nordstrom.kafka.kcr.kafka

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.io.SourceFactory
import java.util.*

class KafkaSourceFactory(
    sourceConfig: Properties,
    private val topic: String,
    groupId: String?
) : SourceFactory {
    private val config: Properties = Properties()
    private val keyGen = AlphaNumKeyGenerator()
    private val id: String

    init {
        config.putAll(sourceConfig)
        id = keyGen.key(8)
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