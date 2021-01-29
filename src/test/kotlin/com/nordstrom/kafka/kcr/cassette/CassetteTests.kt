package com.nordstrom.kafka.kcr.cassette

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import com.nordstrom.kafka.kcr.io.FileSinkFactory
import com.nordstrom.kafka.kcr.io.NullSinkFactory
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.io.File
import java.nio.file.Files
import kotlin.io.path.ExperimentalPathApi
import kotlin.test.*


class CassetteTests {

    private var folder = Files.createTempDirectory("CassetteTests").toFile().absolutePath
    private val keyGen = AlphaNumKeyGenerator()


    @Test
    fun `nop test`() {
        val message = "Il est mort, Jean Luc"
        assertTrue(true)
        assertFalse(false)
        assertDoesNotThrow { }
        assertFails { throw Exception(message) }
        assertFailsWith<Exception> { throw Exception(message) }
        assertThrows<Exception> { throw Exception(message) }
    }

    @Test
    fun `Cassette topic cannot be null or blank`() {
        val e1 = assertFailsWith<IllegalArgumentException> {
            Cassette(topic = null, dataDirectory = null)
        }
        e1.message?.contains("Topic cannot be null or blank")?.let { assertTrue(it, "null topic check") }

        val e2 = assertFailsWith<IllegalArgumentException> {
            Cassette(topic = "", dataDirectory = null)
        }
        e2.message?.contains("Topic cannot be null or blank")?.let { assertTrue(it, "blank topic check") }
    }

    @Test
    fun `Cassette partitions must be greater-than 0`() {
        val e1 = assertFailsWith<IllegalArgumentException> {
            Cassette(topic = keyGen.key(8), partitions = 0, dataDirectory = null)
        }
        e1.message?.contains("Number of partitions must be > 0")?.let { assertTrue(it, "partition greater than 0 check ") }
    }

    @Test
    fun `Can instantiate a Cassette`() {
        val topic = keyGen.key(8)
        val cassette =
            Cassette(
                topic = topic,
                partitions = 55,
                sinkFactory = NullSinkFactory(),
                dataDirectory = folder
            )
        assertEquals(topic, cassette.topic, "topic")
        assertEquals(55, cassette.partitions, "partitions")
    }

    @Test
    fun `Can create a FileSink Cassette`() {
        val topic = keyGen.key(8)
        val cassette =
            Cassette(
                topic = topic,
                partitions = 2,
                sinkFactory = FileSinkFactory(),
                dataDirectory = folder
            )
        assertTrue(File(folder).isDirectory, "$folder is dir")
        cassette.create(keyGen.key(8))
        assertTrue(File(cassette.cassetteDir).isDirectory)
        assertEquals(2, cassette.sinks.size)
    }
}