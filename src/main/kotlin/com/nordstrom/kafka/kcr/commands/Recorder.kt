package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import com.nordstrom.kafka.kcr.io.Sink
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.kafka.KafkaSource
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.time.Duration

class Recorder(
    private val source: Source?,
    val sink: Sink?
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
                    val record = CassetteRecord(
                        timestamp = it.timestamp(),
                        partition = it.partition(),
                        offset = it.offset(),
                        key = k,
                        value = it.value().toString(Charset.defaultCharset())
                    )
                    it.headers().forEach { header ->
                        record.headers[header.key()] = String(header.value())
                    }
                    @UseExperimental(kotlinx.serialization.UnstableDefault::class)
                    val data = Json.stringify(CassetteRecord.serializer(), record)
                    sink?.writeText("$data\n")
                    metricWrite.increment()
                    metricWriteTotal.increment()
                }
            }
        }
    }

}