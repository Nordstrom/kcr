package com.nordstrom.kafka.kcr.cassette

import com.nordstrom.kafka.kcr.io.Sink
import com.nordstrom.kafka.kcr.io.SinkFactory
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.io.SourceFactory
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import java.util.*

class Cassette(
    val dataDirectory: String?,
    val topic: String? = null,
    val partitions: Int? = 1,
    val sourceFactory: SourceFactory? = null,
    val sinkFactory: SinkFactory? = null
) {
    private val log = LoggerFactory.getLogger(javaClass)

    lateinit var cassetteDir: String
    lateinit var cassetteName: String
    lateinit var manifest: CassetteManifest
    var sinks: MutableList<Sink?> = mutableListOf<Sink?>()
    var sources: MutableList<Source?> = mutableListOf<Source?>()

    init {
        when {
            topic.isNullOrBlank() -> throw IllegalArgumentException("Topic cannot be null or blank")
            partitions!! <= 0 -> throw IllegalArgumentException("Number of partitions must be > 0")
            sinkFactory == null -> throw IllegalArgumentException("Must have a concrete SinkFactory")
//            sourceFactory == null -> throw IllegalArgumentException("Must have a concrete SourceFactory")
        }
    }

    fun create(id: String) {
        val dateFormat = SimpleDateFormat("yyyyMMdd-HHmm", Locale.getDefault())
        dateFormat.timeZone = TimeZone.getTimeZone("UTC")
        val nowish = Date()
        cassetteName = "kcr-$topic-${dateFormat.format(nowish)}"
        cassetteDir = "$dataDirectory/$cassetteName"

        // Create manifest
        //TODO use snakeyaml
        manifest = CassetteManifest(
            sinkFactory = sinkFactory!!,
            directory = cassetteDir,
            id = id,
            name = cassetteName,
            partitions = partitions!!,
            topic = topic!!,
            start = Date().toInstant()
        )

        // Create a sink for each partition
        for (partition in 0 until partitions) {
            val partitionName = "$topic-$partition"
            val sink = sinkFactory.create(cassetteDir, partitionName)
            sinks.add(sink)
        }

        // Create a source for each partition
        for (partition in 0 until partitions) {
            val source = sourceFactory?.create(partition)
            sources.add(source)
        }
    }

}