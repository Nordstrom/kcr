package com.nordstrom.kafka.kcr.io

class FileSinkFactory : SinkFactory {

    override fun create(parent: String?, name: String): Sink? {
        val sink = FileSink(parent = parent, name = name)
        return sink
    }

}