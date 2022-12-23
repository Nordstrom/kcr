package com.nordstrom.kafka.kcr.commands

import com.nordstrom.kafka.kcr.commands.PlaybackLimiter
import com.nordstrom.kafka.kcr.cassette.CassetteRecord
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlin.test.*

class PlaybackLimiterTests {
    @Test
    fun `Should propose no delay for default playback rate`() {
        val playbackLimiter = PlaybackLimiter(0f, 0, clock(0L))

        val record1 = record(1000)
        val record2 = record(2000)

        assertNull(playbackLimiter.proposeDelay(record1))
        assertNull(playbackLimiter.proposeDelay(record2))
    }

    @Test
    fun `Should propose same delay for same playback rate`() {
        val playbackLimiter = PlaybackLimiter(1.0f, 0, clock(0L))

        val record1 = record(1000)
        val record2 = record(2000)

        assertNull(playbackLimiter.proposeDelay(record1))
        assertEquals(Duration.ofMillis(1000), playbackLimiter.proposeDelay(record2))
    }

    @Test
    fun `Should propose delay for slower playback rate`() {
        val playbackLimiter = PlaybackLimiter(0.5f, 0, clock(0L))

        val record1 = record(1000)
        val record2 = record(2000)

        assertNull(playbackLimiter.proposeDelay(record1))
        assertEquals(Duration.ofMillis(2000), playbackLimiter.proposeDelay(record2))
    }

    @Test
    fun `Should propose delay for faster playback rate`() {
        val playbackLimiter = PlaybackLimiter(2.0f, 0, clock(0L))

        val record1 = record(1000)
        val record2 = record(2000)

        assertNull(playbackLimiter.proposeDelay(record1))
        assertEquals(Duration.ofMillis(500), playbackLimiter.proposeDelay(record2))
    }

    fun record(timestamp: Long): CassetteRecord =
        CassetteRecord(mutableMapOf<String, String>(), timestamp, 0, 0, null, "")
    fun clock(epochMillis: Long): Clock =
        Clock.fixed(Instant.ofEpochMilli(epochMillis), ZoneId.of("UTC"))
}