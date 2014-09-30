package com.spotify.heroic.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.spotify.heroic.aggregationcache.AggregationCacheConfig;
import com.spotify.heroic.cluster.ClusterManagerConfig;
import com.spotify.heroic.consumer.ConsumerConfig;
import com.spotify.heroic.http.HttpClientManagerConfig;
import com.spotify.heroic.metadata.MetadataBackendManagerConfig;
import com.spotify.heroic.metric.MetricBackendManagerConfig;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";

    private final ClusterManagerConfig cluster;
    private final MetricBackendManagerConfig metrics;
    private final MetadataBackendManagerConfig metadata;
    private final List<ConsumerConfig> consumers;
    private final AggregationCacheConfig cache;
    private final HttpClientManagerConfig client;
    private final int port;
    private final String refreshClusterSchedule;

    @JsonCreator
    public static HeroicConfig create(
            @JsonProperty("cluster") ClusterManagerConfig cluster,
            @JsonProperty("metrics") MetricBackendManagerConfig metrics,
            @JsonProperty("metadata") MetadataBackendManagerConfig metadata,
            @JsonProperty("consumers") List<ConsumerConfig> consumers,
            @JsonProperty("cache") AggregationCacheConfig cache,
            @JsonProperty("client") HttpClientManagerConfig client,
            @JsonProperty("port") Integer port,
            @JsonProperty("refreshClusterSchedule") String refreshClusterSchedule) {
        if (client == null)
            client = HttpClientManagerConfig.create();

        if (refreshClusterSchedule == null)
            refreshClusterSchedule = DEFAULT_REFRESH_CLUSTER_SCHEDULE;

        if (port == null)
            port = DEFAULT_PORT;

        if (consumers == null)
            consumers = new ArrayList<>();

        return new HeroicConfig(cluster, metrics, metadata, consumers, cache,
                client, port, refreshClusterSchedule);
    }

    public static HeroicConfig parse(Path path, HeroicReporter reporter)
            throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(Files.newInputStream(path), HeroicConfig.class);
    }
}
