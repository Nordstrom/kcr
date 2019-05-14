package com.nordstrom.kafka.kcr.io

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.matchers.string.shouldEndWith
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.io.File
import kotlin.test.assertNotNull

class FileSinkTests : StringSpec({

    val folder = createTempDir()
    val keyGen = AlphaNumKeyGenerator()

    "Can create a FileSink" {
        val suffix = keyGen.key(8)
        val sink = FileSink(name = "${folder.absolutePath}$suffix")
        assertNotNull(sink)

        sink.path.shouldEndWith(suffix)
    }

    "Trying to create a FileSink that already exists throws an exception" {
        val filename = keyGen.key(8)
        val f = File(folder, filename)
        f.createNewFile()

        val exception = shouldThrow<FileAlreadyExistsException> {
            FileSink(name = f.absolutePath)
        }
        exception.message shouldContain (filename)
    }

    "Can create a FileSink with a parent folder" {
        val filename = keyGen.key(8)
        val parentDir = folder.absolutePath
        val sink = FileSink(parent = parentDir, name = filename)

        val p = File(parentDir)
        p.isDirectory.shouldBe(true)

        sink.path.shouldEndWith("/$filename")
        val f = File(sink.path)
        f.isFile.shouldBe(true)
    }
    "Can create a FileSink with a parent folder path" {
        val filename = keyGen.key(8)
        val d1 = keyGen.key(8)
        val d2 = keyGen.key(8)
        val d3 = keyGen.key(8)
        val parentDir = "${folder.absolutePath}/$d1/$d2/$d3"
        val sink = FileSink(parent = parentDir, name = filename)

        val p = File(parentDir)
        p.isDirectory.shouldBe(true)
        p.absolutePath.shouldEndWith("/$d1/$d2/$d3")

        val f = File(sink.path)
        f.isFile.shouldBe(true)
        f.absolutePath.shouldEndWith("/$d1/$d2/$d3/$filename")
    }

}) {
    // add functions here
}