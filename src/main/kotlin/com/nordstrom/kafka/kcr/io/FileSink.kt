package com.nordstrom.kafka.kcr.io

import java.io.File

class FileSink(var parent: String? = null, var name: String?) : Sink {
    override var path: String
        get() = file.absolutePath
        set(value) {}

    val file: File

    init {
        if (parent.isNullOrBlank()) {
            file = File(parent, name)
            val isFileCreated = file.createNewFile()
            if (!isFileCreated) {
                // File already exists
                throw FileAlreadyExistsException(file)
            }
        } else {
            // Create parent directory structure
            val parentDir = File(parent)
            parentDir.mkdirs()
            file = File(parentDir, name)
            val isFileCreated = file.createNewFile()
            if (!isFileCreated) {
                throw FileAlreadyExistsException(file)
            }
        }
    }

    override fun writeText(text: String) {
        file.appendText(text)
    }

    override fun writeBytes(bytes: ByteArray) {
        return file.appendBytes(bytes)
    }

}