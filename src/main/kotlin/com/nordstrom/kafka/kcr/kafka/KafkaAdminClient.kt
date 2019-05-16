package com.nordstrom.kafka.kcr.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.slf4j.LoggerFactory
import java.util.*

// Admin client to retrieve topic metadata (number of partitions, etc).

class KafkaAdminClient(config: Properties) {
    private val log = LoggerFactory.getLogger(javaClass)

    private var client: AdminClient

    init {
        val adminConfig = Properties()
        adminConfig.putAll(config)
        adminConfig["client.id"] = "kcr-admin-cid"

        client = AdminClient.create(adminConfig)

        log.trace("init.ok")
    }

    fun numberPartitions(topic: String): Int {
        val results = client.describeTopics(listOf(topic))
        return results.all().get()[topic]?.partitions()?.size!!
    }

}
