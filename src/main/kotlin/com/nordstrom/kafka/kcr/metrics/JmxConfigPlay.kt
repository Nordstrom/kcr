package com.nordstrom.kafka.kcr.metrics

import io.micrometer.jmx.JmxConfig
import java.time.Duration

class JmxConfigPlay : JmxConfig {
    override fun get(key: String): String? {
        return null
    }

    override fun step(): Duration {
        return Duration.ofSeconds(10)
    }

    override fun prefix(): String {
        return "kcr-jmx"
    }

    override fun domain(): String {
        return "kcr.play"
    }

}
