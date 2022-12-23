package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import java.time.Duration
import java.time.Instant
import java.time.Clock

class PlaybackLimiter(
    val playbackRate: Float,
    val timeshiftNanos: Long,
    val clock: Clock = Clock.systemUTC()
) {
    var previousTimestamp: Instant? = null

    fun proposeDelay(record: CassetteRecord): Duration? {
        return if (playbackRate == 0.0f) {
            return null
        } else {
            val recordTimestamp = Instant.ofEpochMilli(record.timestamp)
            val shiftedTimestamp = recordTimestamp.plusNanos(timeshiftNanos)

            if (previousTimestamp == null) {
                previousTimestamp = shiftedTimestamp
                null
            } else if (playbackRate < 1.0f) {
                val delta = Duration.between(previousTimestamp ?: Instant.now(clock), shiftedTimestamp)
                Duration.ofMillis((delta.toMillis() / playbackRate).toLong()).also {
                    previousTimestamp = shiftedTimestamp
                }
            } else if (playbackRate > 1.0f) {
                val delta = Duration.between(previousTimestamp ?: Instant.now(clock), shiftedTimestamp)
                Duration.ofMillis((delta.toMillis() / playbackRate).toLong()).also {
                    previousTimestamp = shiftedTimestamp.plus(it)
                }
            } else {
                Duration.between(previousTimestamp, shiftedTimestamp)
            }
        }
    }
}