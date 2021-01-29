package com.nordstrom.kafka.kcr.io

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class FileSinkFactoryTests {

    private var folder = Files.createTempDirectory("CassetteTests").toFile().absolutePath
    private val sinkFactory = FileSinkFactory()
    private val keyGen = AlphaNumKeyGenerator()

    @Test
    fun `Factory can create a FileSink with no parent`() {
        val suffix = keyGen.key(8)
        val sink = sinkFactory.create(name = "$folder$suffix")
        assertNotNull(sink)
        when (sink) {
            is FileSink -> {
                assertNotNull(sink.file)
                assertTrue(sink.path.endsWith(suffix))
            }
        }
    }

    @Test
    fun `Trying to create an existing FileSink throws an exception`() {
        val filename = keyGen.key(8)
        val f = File(folder, filename)
        f.createNewFile()

        val e1 = assertFailsWith<FileAlreadyExistsException> {
            FileSink(name = f.absolutePath)
        }
        e1.message?.contains(filename)?.let { kotlin.test.assertTrue(it, "file name") }
    }

}