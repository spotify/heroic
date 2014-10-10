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
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.http.HttpClientManagerModule;
import com.spotify.heroic.ingestion.IngestionModule;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";

    private final int port;
    private final String refreshClusterSchedule;

    private final ClusterManagerModule clusterManagerModule;
    private final MetricManagerModule metricModule;
    private final MetadataManagerModule metadataModule;
    private final AggregationCacheModule aggregationCacheModule;
    private final HttpClientManagerModule httpClientManagerModule;
    private final IngestionModule ingestionModule;
    private final List<ConsumerModule> consumers;

    @JsonCreator
    public static HeroicConfig create(@JsonProperty("port") Integer port,
            @JsonProperty("refreshClusterSchedule") String refreshClusterSchedule,
            @JsonProperty("cluster") ClusterManagerModule cluster, @JsonProperty("metrics") MetricManagerModule metrics,
            @JsonProperty("metadata") MetadataManagerModule metadata, @JsonProperty("cache") AggregationCacheModule cache,
            @JsonProperty("client") HttpClientManagerModule client,
            @JsonProperty("ingestion") IngestionModule ingestion,
            @JsonProperty("consumers") List<ConsumerModule> consumers) {
        if (port == null)
            port = DEFAULT_PORT;

        if (refreshClusterSchedule == null)
            refreshClusterSchedule = DEFAULT_REFRESH_CLUSTER_SCHEDULE;

        if (cluster == null)
            cluster = ClusterManagerModule.createDefault();

        if (metrics == null)
            metrics = MetricManagerModule.createDefault();

        if (metadata == null)
            metadata = MetadataManagerModule.createDefault();

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
}
