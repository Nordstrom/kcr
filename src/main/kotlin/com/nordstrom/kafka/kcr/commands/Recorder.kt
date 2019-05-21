package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.Kcr
import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import com.nordstrom.kafka.kcr.io.Sink
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.kafka.KafkaSource
import io.micrometer.core.instrument.Timer
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.time.Duration

class Recorder(
    private val source: Source?,
    val sink: Sink?
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun record(partitionNumber: Int) {
        val writes = Kcr.registry.counter("kcr.recorder.partition.write-total", "partition", "$partitionNumber")
        log.trace(".record(partition=$partitionNumber)")
        val duration = Timer.start()
        if (source is KafkaSource) {
            source.assign()
            while (true) {
                val records = source.poll(Duration.ofSeconds(20))
                records?.iterator()?.forEach {
                    val record = CassetteRecord(
                        timestamp = it.timestamp(),
                        partition = it.partition(),
                        offset = it.offset(),
                        key = it.key().toString(Charset.defaultCharset()),
                        value = it.value().toString(Charset.defaultCharset())
                    )
                    it.headers().forEach { header ->
                        record.headers[header.key()] = String(header.value())
                    }
                    @UseExperimental(kotlinx.serialization.UnstableDefault::class)
                    val data = Json.stringify(CassetteRecord.serializer(), record)
                    sink?.writeText("$data\n")
                    writes.increment()
//                    log.trace(".record:$data")
                }
            }
        }

        duration.stop(
            Kcr.registry.timer(
                "kcr.recorder.partition.duration-ms",
                "partition", "$partitionNumber"
            )
        )
        log.trace(".record.OK")
    }

}