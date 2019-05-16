package com.nordstrom.kafka.kcr.io

interface Source {

    fun readBytes(): ByteArray

}