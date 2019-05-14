package com.nordstrom.kafka.kcr.io

import com.nordstrom.kafka.kcr.facilities.AlphaNumKeyGenerator
import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.matchers.string.shouldEndWith
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.io.File
import javax.crypto.KeyGenerator
import kotlin.test.assertNotNull

class FileSinkFactoryTests : StringSpec({

    val folder = createTempDir()
    val sinkFactory = FileSinkFactory()
    val keyGen = AlphaNumKeyGenerator()

    "Factory can create a FileSink with no parent" {
        val suffix = keyGen.key(8)
        val sink = sinkFactory.create(name = "${folder.absolutePath}$suffix")
        assertNotNull(sink)

        if (sink is FileSink) {
            sink.file.shouldNotBe(null)
            sink.path.shouldEndWith(suffix)
        }
    }

    "Trying to create an existing FileSink throws an exception" {
        val filename = keyGen.key(8)
        val f = File(folder, filename)
        f.createNewFile()

        val exception = shouldThrow<FileAlreadyExistsException> {
            FileSink(name = f.absolutePath)
        }
        exception.message shouldContain (filename)
    }

}) {
    // add functions here
}