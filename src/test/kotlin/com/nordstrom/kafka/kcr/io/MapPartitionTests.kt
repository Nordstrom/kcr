package com.nordstrom.kafka.kcr.io

import io.kotlintest.matchers.numerics.shouldBeBetween
import io.kotlintest.specs.StringSpec

class MapPartitionTests : StringSpec({

    "Can map larger to smaller" {
        for (i in 0..LARGER_TOPIC) {
            (i % SMALLER_TOPIC).shouldBeBetween(0, SMALLER_TOPIC)
//            println("${i % SMALLER_TOPIC}")
        }
    }
}) {

    companion object {
        val SMALLER_TOPIC = 5
        val LARGER_TOPIC = 10
    }

}