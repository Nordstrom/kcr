package com.nordstrom.kafka.kcr.cassette

import kotlinx.serialization.Serializable

@Serializable
class CassetteRecord(
    val headers: MutableMap<String, String> = mutableMapOf<String, String>(),
    var timestamp: Long,
    val partition: Int,
    val offset: Long,
    val key: String? = null,
    val value: String
) {
    fun withHeaderTimestamp(key: String) {
        if (headers.containsKey(key)) {
            timestamp = headers[key]?.toLong() ?: timestamp
        }
    }
}
