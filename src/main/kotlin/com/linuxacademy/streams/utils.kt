package com.linuxacademy.streams

import java.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toJavaDuration

// Integer extension function
fun Int.seconds(): Duration {
    return this.toDuration(DurationUnit.SECONDS).toJavaDuration()
}
