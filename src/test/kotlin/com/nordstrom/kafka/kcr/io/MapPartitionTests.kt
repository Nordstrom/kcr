package com.nordstrom.kafka.kcr.io

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class MapPartitionTests {

    @Test
    fun `Can map larger to smaller`() {
        for (i in 0..LARGER_TOPIC) {
            assertTrue((i % SMALLER_TOPIC) >= 0)
            assertTrue((i % SMALLER_TOPIC) <= SMALLER_TOPIC)
        }
    }

    companion object {
        const val SMALLER_TOPIC = 5
        const val LARGER_TOPIC = 10
    }

}