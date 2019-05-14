package com.nordstrom.kafka.kcr.commands

import java.util.*

class Recorder(
    private val config: Properties,
    private val cassetteName: String?,  //ignore for now
    private val groupId: String?,
    private val topic: String,
    private val numberPartitions: Int
) {

    init {
        println("kcr-recorder.init")
    }

    fun record() {
//        println("kcr-recorder.record")
//        for (source in cassette!!.sources) {
//            if (source is KafkaSource) {
//                source.client.subscribe(listOf(topic))
//
//                while (true) {
//                    val records = source.client.poll(Duration.ofSeconds(20))
//                    records.iterator().forEach {
//                        println("val=$it.value()")
//                    }
//                }
//            }
//        }
    }


}