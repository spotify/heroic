package com.spotify.heroic.shell.task

data class WritePerformanceTimes(
    val executionTimes: List<Long>,
    val runtime: Long
)

data class WritePerformanceCollectedTimes(
    val runtimes: List<Long>,
    val executionTimes: List<Long>,
    val errors: Int
)