package com.nordstrom.kafka.kcr.kafka

import org.apache.kafka.clients.admin.AdminClient
import java.util.*

// Admin client to retrieve topic metadata (number of partitions, etc).

class KafkaAdminClient(config: Properties) {
    private var client : AdminClient

    init {
        val adminConfig = Properties()
        adminConfig.putAll(config)
        adminConfig["client.id"] = "kcr-admin-cid"

        client = AdminClient.create(adminConfig)
    }

    fun numberPartitions(topic: String): Int {
        val results = client.describeTopics(listOf(topic))
        return results.all().get()[topic]?.partitions()?.size!!
    }

}
