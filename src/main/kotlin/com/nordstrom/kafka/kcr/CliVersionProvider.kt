package com.nordstrom.kafka.kcr

import picocli.CommandLine
import java.util.*

class CliVersionProvider : CommandLine.IVersionProvider {
    val properties = Properties()

    init {
        CliVersionProvider::class.java.classLoader.getResourceAsStream("version.properties").use {
            properties.load(it)
        }
    }

    override fun getVersion() = arrayOf("kcr ${properties.getProperty("version")}");
}