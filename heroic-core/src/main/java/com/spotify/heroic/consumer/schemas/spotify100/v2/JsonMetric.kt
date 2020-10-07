package com.spotify.heroic.consumer.schemas.spotify100.v2

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class JsonMetric(
        val key: String?,
        @Deprecated("Host should be set as a normal tag")
        val host: String?,
        val time: Long?,
        @JsonProperty("attributes", access = JsonProperty.Access.WRITE_ONLY)
        val rawAttributes: Map<String, String?> = emptyMap(),
        @JsonProperty("resource", access = JsonProperty.Access.WRITE_ONLY)
        val rawResource: Map<String, String?>?,
        val value: Value?
) {
    @Suppress("UNCHECKED_CAST")
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    val attributes = rawAttributes.filter { it.value != null } as Map<String, String>

    @Suppress("UNCHECKED_CAST")
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    val resource =
            (rawResource ?: emptyMap()).filter { it.value != null } as Map<String, String>
}
