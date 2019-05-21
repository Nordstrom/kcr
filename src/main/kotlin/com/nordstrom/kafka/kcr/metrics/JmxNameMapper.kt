package com.nordstrom.kafka.kcr.metrics

import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.config.NamingConvention
import io.micrometer.core.instrument.util.HierarchicalNameMapper

class JmxNameMapper : HierarchicalNameMapper {
    override fun toHierarchicalName(id: Meter.Id, convention: NamingConvention): String {
        var name = id.name
        if (id.tags.isNotEmpty()) {
            val t = id.tags.joinToString { "${it.key}.${it.value}" }
            name += ".$t"
        }

        return name
    }

}