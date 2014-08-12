package com.spotify.heroic.yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.InMemoryAggregationCacheBackend;
import com.spotify.heroic.cache.cassandra.CassandraCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.discovery.StaticListDiscovery;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.kafka.KafkaConsumer;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.heroic.HeroicMetricBackend;
import com.spotify.heroic.metrics.kairosdb.KairosMetricBackend;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
public class HeroicConfig {
    public static final long MAX_AGGREGATION_MAGNITUDE = 300000;
    public static final boolean UPDATE_METADATA = false;
    public static final long MAX_QUERIABLE_DATA_POINTS = 100000;
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";
    public static final int DEFAULT_GROUP_LIMIT = 500;
    public static final int DEFAULT_GROUP_LOAD_LIMIT = 5000;

    @Getter
    private final ClusterManager cluster;

    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<MetadataBackend> metadataBackends;

    @Getter
    private final List<Consumer> consumers;

    @Getter
    private final AggregationCache aggregationCache;

    @Getter
    private final long maxAggregationMagnitude;

    @Getter
    private final boolean updateMetadata;

    @Getter
    private final int port;

    @Getter
    private final String refreshClusterSchedule;

    @Getter
    private final int groupLimit;

    @Getter
    private final int groupLoadLimit;

    private static final TypeDescription[] TYPES = new TypeDescription[] {
        Utils.makeType(HeroicMetricBackend.YAML.class),
        Utils.makeType(KairosMetricBackend.YAML.class),
        Utils.makeType(InMemoryAggregationCacheBackend.YAML.class),
        Utils.makeType(CassandraCache.YAML.class),
        Utils.makeType(ElasticSearchMetadataBackend.YAML.class),
        Utils.makeType(KafkaConsumer.YAML.class),
        Utils.makeType(StaticListDiscovery.YAML.class) };

    private static final class CustomConstructor extends Constructor {
        public CustomConstructor() {
            for (final TypeDescription t : TYPES) {
                addTypeDescription(t);
            }
        }
    }

    public static HeroicConfig buildDefault(HeroicReporter reporter) {
        final AggregationCache cache = new AggregationCache(
                reporter.newAggregationCache(null),
                new InMemoryAggregationCacheBackend());
        final ClusterManager cluster = new ClusterManager(null, UUID.randomUUID(), null);
        final List<MetricBackend> metricBackends = new ArrayList<MetricBackend>();
        final List<MetadataBackend> metadataBackends = new ArrayList<MetadataBackend>();
        final List<Consumer> consumers = new ArrayList<Consumer>();
        return new HeroicConfig(cluster, metricBackends, metadataBackends, consumers,
                cache, MAX_AGGREGATION_MAGNITUDE, UPDATE_METADATA,
                DEFAULT_PORT, DEFAULT_REFRESH_CLUSTER_SCHEDULE,
                DEFAULT_GROUP_LIMIT,
                DEFAULT_GROUP_LOAD_LIMIT);
    }

    public static HeroicConfig parse(Path path, HeroicReporter reporter)
            throws ValidationException, IOException {
        final Yaml yaml = new Yaml(new CustomConstructor());

        final HeroicConfigYAML configYaml = yaml.loadAs(
                Files.newInputStream(path), HeroicConfigYAML.class);

        if (configYaml == null) {
            return HeroicConfig.buildDefault(reporter);
        }

        return configYaml.build(reporter);
    }
}
