package com.nordstrom.kafka.kcr.io

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

class FileSinkTests {

    private var folder = Files.createTempDirectory("CassetteTests").toFile().absolutePath
    private val keyGen = AlphaNumKeyGenerator()

    @Test
    fun `Can create a FileSink`() {
        val suffix = keyGen.key(8)
        val sink = FileSink(name = "$folder$suffix")
        assertNotNull(sink)
        assertTrue(sink.path.endsWith(suffix))
    }

    @Test
    fun
            `Trying to create a FileSink that already exists throws an exception`() {
        val filename = keyGen.key(8)
        val f = File(folder, filename)
        f.createNewFile()
        val e1 = assertFailsWith<FileAlreadyExistsException> {
            FileSink(name = f.absolutePath)
        }
        e1.message?.contains(filename)?.let { kotlin.test.assertTrue(it, "file name") }
    }

    @Test
    fun `Can create a FileSink with a parent folder`() {
        val filename = keyGen.key(8)
        val parentDir = folder
        val sink = FileSink(parent = parentDir, name = filename)

        val p = File(parentDir)
        assertTrue(p.isDirectory)
        assertTrue(sink.path.endsWith("/$filename"))
        val f = File(sink.path)
        assertTrue(f.isFile)
    }

    @Test
    fun `Can create a FileSink with a parent folder path`() {
        val filename = keyGen.key(8)
        val d1 = keyGen.key(8)
        val d2 = keyGen.key(8)
        val d3 = keyGen.key(8)
        val parentDir = "$folder/$d1/$d2/$d3"
        val sink = FileSink(parent = parentDir, name = filename)

        val p = File(parentDir)
        assertTrue(p.isDirectory)
        assertTrue(p.absolutePath.endsWith("/$d1/$d2/$d3"))

        val f = File(sink.path)
        assertTrue(f.isFile)
        assertTrue(f.absolutePath.endsWith("/$d1/$d2/$d3/$filename"))
    }

}
