package com.spotify.heroic.aggregationcache;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Module;
import com.spotify.heroic.aggregationcache.cassandra.CassandraCacheConfig;

/**
 * Is used to query for pre-aggregated cached time series.
 *
 * @author udoprog
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = InMemoryAggregationCacheBackendConfig.class, name = "in-memory"),
        @JsonSubTypes.Type(value = CassandraCacheConfig.class, name = "cassandra") })
public interface AggregationCacheBackendConfig {
    public Module module();
}
