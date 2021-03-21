package io.lantern.messaging.time

val Long.secondsToMillis get() = this * 1000

val Long.minutesToSeconds get() = this * 60

val Long.minutesToMillis get() = this.minutesToSeconds.secondsToMillis

val Long.millisToNanos get() = this * 1000000

val Long.nanosToMillis get() = this / 1000000
