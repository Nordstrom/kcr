package com.nordstrom.kafka.kcr.cassette

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import com.nordstrom.kafka.kcr.io.FileSinkFactory
import com.nordstrom.kafka.kcr.io.NullSinkFactory
import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.io.File

class CassetteTests : StringSpec({

    val folder = createTempDir()
    val keyGen = AlphaNumKeyGenerator()

    "Cassette topic cannot be null or blank" {
        var exception = shouldThrow<IllegalArgumentException> {
            Cassette(topic = null, dataDirectory = null)
        }
        exception.message shouldContain ("Topic cannot be null or blank")

        exception = shouldThrow<IllegalArgumentException> {
            Cassette(topic = "", dataDirectory = null)
        }
        exception.message shouldContain ("Topic cannot be null or blank")
    }

    "Cassette partitions must be > 0" {
        val exception = shouldThrow<IllegalArgumentException> {
            Cassette(topic = keyGen.key(8), partitions = 0, dataDirectory = null)
        }
        exception.message shouldContain ("Number of partitions must be > 0")
    }

    "Can instantiate a Cassette" {
        val topic = keyGen.key(8)
        val cassette =
            Cassette(topic = topic, partitions = 55, sinkFactory = NullSinkFactory(), dataDirectory = folder.absolutePath)
        cassette.topic.shouldBe(topic)
        cassette.partitions.shouldBe(55)
    }

    "Can create a FileSink Cassette" {
        val topic = keyGen.key(8)
        val cassette =
            Cassette(topic = topic, partitions = 2, sinkFactory = FileSinkFactory(), dataDirectory = folder.absolutePath)
        cassette.create()
        File(cassette.cassetteDir).isDirectory.shouldBe(true)
        File(cassette.cassetteDir, "$topic-0").isFile.shouldBe(true)
        File(cassette.cassetteDir, "$topic-1").isFile.shouldBe(true)
        File(cassette.cassetteDir, "$topic-2").isFile.shouldNotBe(true) //not created!

        cassette.sinks.size.shouldBe(2)
    }

}) {
    // add functions here
}