package com.nordstrom.kafka.kcr.io

interface Sink {

    var path: String


    fun writeText(text: String)

    fun writeBytes(bytes: ByteArray)

}