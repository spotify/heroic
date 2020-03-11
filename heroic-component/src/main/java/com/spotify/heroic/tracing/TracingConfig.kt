package com.spotify.heroic.tracing

data class TracingConfig(
    val probability: Double = 0.01,
    val zpagesPort: Int = 0,
    val requestHeadersToTags: List<String> = listOf(),
    val tags: Map<String, String> = mapOf(),
    val lightstep: Lightstep = Lightstep(),
    val squash: Squash = Squash()
) {
    data class Lightstep(
        val collectorHost: String = "",
        val collectorPort: Int = 80,
        val accessToken: String = "",
        val componentName: String = "heroic",
        val reportingIntervalMs: Int = 1000,
        val maxBufferedSpans: Int = 5000,
        val resetClient: Boolean = false
    )

    data class Squash(
        val whitelist: List<String>? = listOf(),
        val threshold: Int = 100
    )
}
