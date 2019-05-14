package com.nordstrom.kafka.kcr.cassette

import kotlinx.serialization.Serializable

@Serializable
class CassetteRecord(
    val headers: MutableMap<String, String> = mutableMapOf<String, String>(),
    val timestamp: Long,
    val partition: Int,
    val offset: Long,
    val key: String?,
    val value: String
)
