package com.nordstrom.kafka.kcr.metrics

import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.core.instrument.util.HierarchicalNameMapper
import io.micrometer.jmx.JmxConfig
import java.time.Duration

class JmxConfigRecord : JmxConfig {
    override fun get(key: String): String? {
        return null
    }

    override fun step(): Duration {
        return Duration.ofSeconds(10)
    }

    override fun prefix(): String {
        return "jmx"
    }

    override fun domain(): String {
        return DOMAIN
    }

    companion object {
        val DOMAIN = "kcr.recorder"
    }
}
