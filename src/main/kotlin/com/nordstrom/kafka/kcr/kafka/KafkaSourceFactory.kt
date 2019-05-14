package com.nordstrom.kafka.kcr.kafka

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.io.SourceFactory
import org.slf4j.LoggerFactory
import java.util.*

class KafkaSourceFactory(
    sourceConfig: Properties,
    private val topic: String,
    groupId: String?
    //TODO id:String // cassette id
) : SourceFactory {
    private val log = LoggerFactory.getLogger(javaClass)

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

        log.trace(".init.ok:gid=$gid")
    }

    override fun create(partition: Int): Source {
        // Unique cid
        val cid = "kcr-$topic-cid-$id-$partition"
        config["client.id"] = cid
        log.trace(".create:cid=$cid")

        return KafkaSource(config, topic, partition)
    }

}