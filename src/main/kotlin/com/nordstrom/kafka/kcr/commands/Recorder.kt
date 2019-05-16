package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import com.nordstrom.kafka.kcr.io.Sink
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.kafka.KafkaSource
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Duration

class Recorder(
    private val source: Source?,
    val sink: Sink?
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun record() {
        log.trace(".record")
        if (source is KafkaSource) {
            source.assign()
            while (true) {
                val records = source.poll(Duration.ofSeconds(20))
                records?.iterator()?.forEach {
                    val record = CassetteRecord(
                        timestamp = it.timestamp(),
                        partition = it.partition(),
                        offset = it.offset(),
                        key = it.key(),
                        value = it.value()
                    )
                    it.headers().forEach { header ->
                        record.headers[header.key()] = String(header.value())
                    }
                    val data = Json.stringify(CassetteRecord.serializer(), record)
                    sink?.writeText("$data\n")
                    log.trace(".record:$data")
//                    val ts = Date(it.timestamp())
//                    log.trace(".record:timestamp:${ts.toInstant()}")
                }
            }
        }
        log.trace(".record.ok")
    }

}