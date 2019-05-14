package com.nordstrom.kafka.kcr.io


interface SourceFactory {

    fun create(partition: Int): Source?

}