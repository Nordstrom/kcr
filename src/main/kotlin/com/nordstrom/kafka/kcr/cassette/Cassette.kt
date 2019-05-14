package com.nordstrom.kafka.kcr.cassette

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import com.nordstrom.kafka.kcr.io.Sink
import com.nordstrom.kafka.kcr.io.SinkFactory
import com.nordstrom.kafka.kcr.io.Source
import com.nordstrom.kafka.kcr.io.SourceFactory
import java.lang.IllegalArgumentException
import java.text.SimpleDateFormat
import java.util.*

class Cassette(
    val dataDirectory: String?,
    val topic: String? = null,
    val tracks: Int? = 1,
    val sourceFactory: SourceFactory? = null,
    val sinkFactory: SinkFactory? = null
) {

    val id = AlphaNumKeyGenerator().key(8)

    lateinit var cassetteDir: String
    lateinit var cassetteName: String
    lateinit var manifest: Sink
    var sinks: MutableList<Sink?> = mutableListOf<Sink?>()
    var sources: MutableList<Source?> = mutableListOf<Source?>()

    init {
        when {
            topic.isNullOrBlank() -> throw IllegalArgumentException("Topic cannot be null or blank")
            tracks!! <= 0 -> throw IllegalArgumentException("Number of tracks must be > 0")
            sinkFactory == null -> throw IllegalArgumentException("Must have a concrete SinkFactory")
//            sourceFactory == null -> throw IllegalArgumentException("Must have a concrete SourceFactory")
        }
    }

    fun create() {
        val dateFormat = SimpleDateFormat("yyyyMMdd-HHmm", Locale.getDefault())
        dateFormat.timeZone = TimeZone.getTimeZone("UTC")
        val nowish = Date()
        cassetteName = "kcr-$topic-${dateFormat.format(nowish)}"
        cassetteDir = "$dataDirectory/$cassetteName"

        // Create manifest
        manifest = sinkFactory?.create(cassetteDir, "$topic.manifest")!!
        manifest.writeText("---\n")
        manifest.writeText("version:0.1\n")
        manifest.writeText("id:$id\n")
        manifest.writeText("topic:$topic\n")
        manifest.writeText("partitions=$tracks\n")

        // Create sinks for each track
        for (t in 0 until tracks!!) {
            val trackName = "$topic-$t"
            val track = sinkFactory?.create(cassetteDir, trackName)
            sinks.add(track)
        }

        // Create sources for each track
        for (t in 0 until tracks){
            val track = sourceFactory?.create(t)
            sources.add(track)
        }
    }

}