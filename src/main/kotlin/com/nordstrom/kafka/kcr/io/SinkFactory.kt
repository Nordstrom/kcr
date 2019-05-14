package com.nordstrom.kafka.kcr.io

interface SinkFactory {

    fun create(parent: String? = null, name: String): Sink?

}