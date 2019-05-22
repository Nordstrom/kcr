package com.nordstrom.kafka.kcr.cassette

import kotlinx.serialization.json.Json
import java.io.File
import java.time.Duration
import java.time.Instant
import java.util.*

class CassetteInfo(val cassette: String) {

    class CassettePartitionInfo(val cassette: String, val file: String) {
        val earliest: Long
        val latest: Long
        var count: Int

        init {
            var first = Long.MAX_VALUE
            var last = Long.MIN_VALUE
            val reader = File(cassette, file).bufferedReader()
            count = 0
            reader.useLines { lines ->
                lines.forEach { line ->
                    @UseExperimental(kotlinx.serialization.UnstableDefault::class)
                    val record = Json.parse(CassetteRecord.serializer(), line)
                    if (record.timestamp < first) {
                        first = record.timestamp
                    }
                    if (record.timestamp > last) {
                        last = record.timestamp
                    }
                    count++
                }
            }
            earliest = first
            latest = last
        }
    }

    val earliest: Instant
    private val latest: Instant
    private val partitions: MutableList<CassettePartitionInfo> = mutableListOf()
    val totalRecords: Int

    init {
        val filelist = File(cassette).list()
        for (file in filelist) {
            if ("manifest" in file) {
                continue
            } else {
                val partition = CassettePartitionInfo(cassette, file)
                partitions.add(partition)
            }
        }
        val t0 = partitions.stream().map(CassettePartitionInfo::earliest).min(Long::compareTo)
        val t1 = partitions.stream().map(CassettePartitionInfo::latest).max(Long::compareTo)
        earliest = Date(t0.get()).toInstant()
        latest = Date(t1.get()).toInstant()
        totalRecords = partitions.sumBy { it.count }
    }

    fun summary(): String {
        return """
 _________
|   ___   | title   : $cassette
|  o___o  | tracks  : ${partitions.size}
|__/___\__| songs   : $totalRecords
            recorded: $earliest - $latest
            length  : ${Duration.between(earliest, latest)}
        """.trimIndent()
    }
}