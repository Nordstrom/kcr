package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import com.nordstrom.kafka.kcr.io.Sink
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.kafka.KafkaSource
import io.micrometer.core.instrument.MeterRegistry
import java.nio.charset.Charset
import java.time.Duration
import kotlinx.serialization.*
import kotlinx.serialization.json.Json
import org.apache.commons.codec.binary.Hex
import org.apache.kafka.common.record.TimestampType
import org.slf4j.LoggerFactory
import java.time.Instant

class Recorder(
    private val source: Source?,
    val sink: Sink?,
    private val headerTimestamp: String,
) {
    private val log = LoggerFactory.getLogger(javaClass)


    fun record(partitionNumber: Int, registry: MeterRegistry) {
        val metricWriteTotal = registry.counter("write.total")
        val metricWrite = registry.counter("write.total", "partition", "$partitionNumber")
        if (source is KafkaSource) {
            source.assign()
            while (true) {
                val records = source.poll(Duration.ofSeconds(20))
                records?.iterator()?.forEach {
                    // The record does not always have a key so we need to test for that explicitly.
                    var k: String? = null
                    if (it.key() != null && it.key().isNotEmpty()) {
                        k = it.key().toString(Charset.defaultCharset())
                    }
                    val timestamp = when (it.timestampType()) {
                        TimestampType.NO_TIMESTAMP_TYPE, null -> {
                            Instant.now().toEpochMilli()
                        }
                        else -> {
                            it.timestamp()
                        }
                    }
                    val record = CassetteRecord(
                        timestamp = timestamp,
                        partition = it.partition(),
                        offset = it.offset(),
                        key = k,
                        value = Hex.encodeHex(it.value()).joinToString("")
                    )
                    log.debug(".record: ts=${Instant.ofEpochMilli(it.timestamp())}, type=${it.timestampType()}, p=${it.partition()}, o=${it.offset()}, k=${k}")
                    it.headers().forEach { header ->
                        record.headers[header.key()] = String(header.value())
                    }
                    if (headerTimestamp.isNotBlank()) {
                        record.withHeaderTimestamp(headerTimestamp)
                    }
                    val data = Json.encodeToString(record)
                    log.debug(".record: $data")
                    sink?.writeText("$data\n")
                    metricWrite.increment()
                    metricWriteTotal.increment()
                }
            }
        }
    }

}