package com.spotify.heroic;

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
import com.spotify.heroic.aggregationcache.AggregationCacheModule;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.consumer.ConsumerConfig;
import com.spotify.heroic.http.HttpClientManagerModule;
import com.spotify.heroic.ingestion.IngestionModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";

    private final int port;
    private final String refreshClusterSchedule;

    private final ClusterManagerModule clusterManagerModule;
    private final MetricModule metricModule;
    private final MetadataModule metadataModule;
    private final AggregationCacheModule aggregationCacheModule;
    private final HttpClientManagerModule httpClientManagerModule;
    private final IngestionModule ingestionModule;
    private final List<ConsumerConfig> consumers;

    @JsonCreator
    public static HeroicConfig create(@JsonProperty("port") Integer port,
            @JsonProperty("refreshClusterSchedule") String refreshClusterSchedule,
            @JsonProperty("cluster") ClusterManagerModule cluster, @JsonProperty("metrics") MetricModule metrics,
            @JsonProperty("metadata") MetadataModule metadata, @JsonProperty("cache") AggregationCacheModule cache,
            @JsonProperty("client") HttpClientManagerModule client,
            @JsonProperty("ingestion") IngestionModule ingestion,
            @JsonProperty("consumers") List<ConsumerConfig> consumers) {
        if (port == null)
            port = DEFAULT_PORT;

        if (refreshClusterSchedule == null)
            refreshClusterSchedule = DEFAULT_REFRESH_CLUSTER_SCHEDULE;

        if (cluster == null)
            cluster = ClusterManagerModule.createDefault();

        if (metrics == null)
            metrics = MetricModule.createDefault();

        if (metadata == null)
            metadata = MetadataModule.createDefault();

        if (client == null)
            client = HttpClientManagerModule.createDefault();

        if (cache == null)
            cache = AggregationCacheModule.createDefault();

        if (ingestion == null)
            ingestion = IngestionModule.createDefault();

        if (consumers == null)
            consumers = new ArrayList<>();

        return new HeroicConfig(port, refreshClusterSchedule, cluster, metrics, metadata, cache, client, ingestion,
                consumers);
    }

    public static HeroicConfig parse(Path path, HeroicReporter reporter) throws IOException {
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(Files.newInputStream(path), HeroicConfig.class);
    }
}
