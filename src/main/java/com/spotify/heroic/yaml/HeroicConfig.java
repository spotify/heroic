package com.spotify.heroic.yaml;

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
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";

    private final ClusterManager cluster;
    private final MetricBackendManager metrics;
    private final MetadataBackendManager metadata;
    private final List<Consumer> consumers;
    private final AggregationCache cache;
    private final int port;
    private final String refreshClusterSchedule;

    @JsonCreator
    public static HeroicConfig create(
            @JsonProperty("cluster") ClusterManager cluster,
            @JsonProperty("metrics") MetricBackendManager metrics,
            @JsonProperty("metadata") MetadataBackendManager metadata,
            @JsonProperty("consumers") List<Consumer> consumers,
            @JsonProperty("cache") AggregationCache cache,
            @JsonProperty("port") Integer port,
            @JsonProperty("refreshClusterSchedule") String refreshClusterSchedule) {
        if (cache == null)
            cache = new AggregationCache(null);

        if (refreshClusterSchedule == null)
            refreshClusterSchedule = DEFAULT_REFRESH_CLUSTER_SCHEDULE;

        if (port == null)
            port = DEFAULT_PORT;

        if (consumers == null)
            consumers = new ArrayList<>();

        return new HeroicConfig(cluster, metrics, metadata, consumers, cache,
                port, refreshClusterSchedule);
    }

    public static HeroicConfig parse(Path path, HeroicReporter reporter)
            throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(Files.newInputStream(path), HeroicConfig.class);
    }
}
