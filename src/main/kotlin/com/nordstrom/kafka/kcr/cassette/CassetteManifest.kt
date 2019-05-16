package com.nordstrom.kafka.kcr.cassette

import com.nordstrom.kafka.kcr.io.SinkFactory
import org.slf4j.LoggerFactory
import java.time.Instant

class CassetteManifest(
    sinkFactory: SinkFactory,
    directory: String,
    id: String,
    name: String,
    partitions: Int,
    topic: String,
    start: Instant
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private val manifest = sinkFactory.create(directory, "$topic.manifest")

    init {
        //TODO serialize as yaml
        manifest?.writeText("---\n")
        manifest?.writeText("directory:$directory\n")
        manifest?.writeText("id:$id\n")
        manifest?.writeText("name:$name\n")
        manifest?.writeText("partitions:$partitions\n")
        manifest?.writeText("topic:$topic\n")
        manifest?.writeText("version:${CassetteVersion.VERSION}\n")
        manifest?.writeText("start:${start}")

        log.trace(".init.ok")
    }

    //TODO Add functions to append more info?
}
