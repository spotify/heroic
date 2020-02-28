package com.spotify.heroic.tracing

data class TracingConfig (
    val probability: Double = 0.01,
    val zpagesPort: Int = 0,
    val requestHeadersToTags: List<String> = listOf(),
    val lightstep: Lightstep = Lightstep()
) {
    data class Lightstep (
        val collectorHost: String = "",
        val collectorPort: Int = 80,
        val accessToken: String = "",
        val componentName: String = "heroic",
        val reportingIntervalMs: Int = 1000,
        val maxBufferedSpans: Int = 5000,
        val resetClient: Boolean = false
    )
}
