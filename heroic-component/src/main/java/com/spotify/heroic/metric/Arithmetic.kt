package com.spotify.heroic.metric

/**
 * Encapsulates the Grafana user's arithmetic expression that was entered in the
 * query editor e.g.
 *
 * { "C" → "A / B * 100" }
 *
 * where A and B are Grafana query names
 */
data class Arithmetic(val expression: String)
