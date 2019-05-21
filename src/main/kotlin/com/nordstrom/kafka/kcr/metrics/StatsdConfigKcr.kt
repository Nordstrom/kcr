package com.nordstrom.kafka.kcr.metrics

import io.micrometer.statsd.StatsdConfig
import io.micrometer.statsd.StatsdFlavor

class StatsdConfigKcr : StatsdConfig {
    override fun get(key: String): String? {
        return null
    }

    override fun flavor(): StatsdFlavor {
        return StatsdFlavor.ETSY
    }

}
