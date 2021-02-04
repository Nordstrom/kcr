package com.nordstrom.kafka.kcr.commands

import java.util.*

class NullRecorder(
    val config: Properties,
    val cassetteName: String?
) {
    init {
        println("kcr-null-recorder.init.OK")
    }

    fun record() {
        println("kcr-null-recorder.record")
    }
}