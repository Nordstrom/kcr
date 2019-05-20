package com.nordstrom.kafka.kcr.io

class NullSink : Sink {
    override var path: String
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.
        set(_) {}

    override fun writeText(text: String) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun writeBytes(bytes: ByteArray) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}