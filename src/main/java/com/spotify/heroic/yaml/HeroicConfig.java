package com.spotify.heroic.yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.AggregationCacheImpl;
import com.spotify.heroic.cache.InMemoryAggregationCacheBackend;
import com.spotify.heroic.cache.cassandra.CassandraCache;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterManagerImpl;
import com.spotify.heroic.cluster.discovery.StaticListDiscovery;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.kafka.KafkaConsumer;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.heroic.HeroicBackend;
import com.spotify.heroic.metrics.kairosdb.KairosBackend;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final long MAX_AGGREGATION_MAGNITUDE = 300000;
    public static final long MAX_QUERIABLE_DATA_POINTS = 100000;
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";

    private final ClusterManager cluster;
    private final MetricBackendManager metrics;
    private final MetadataBackendManager metadata;
    private final List<Consumer> consumers;
    private final AggregationCache cache;
    private final int port;
    private final String refreshClusterSchedule;

    private static final TypeDescription[] TYPES = new TypeDescription[] {
            ConfigUtils.makeType(HeroicBackend.YAML.class),
            ConfigUtils.makeType(KairosBackend.YAML.class),
            ConfigUtils.makeType(InMemoryAggregationCacheBackend.YAML.class),
            ConfigUtils.makeType(CassandraCache.YAML.class),
            ConfigUtils.makeType(ElasticSearchMetadataBackend.YAML.class),
            ConfigUtils.makeType(KafkaConsumer.YAML.class),
            ConfigUtils.makeType(StaticListDiscovery.YAML.class) };

    private static final class CustomConstructor extends Constructor {
        public CustomConstructor() {
            for (final TypeDescription t : TYPES) {
                addTypeDescription(t);
            }
        }
    }

    public static HeroicConfig buildDefault(HeroicReporter reporter) {
        final AggregationCache cache = new AggregationCacheImpl(
                reporter.newAggregationCache(null),
                new InMemoryAggregationCacheBackend());

        final MetricBackendManager metrics = new MetricBackendManager(
                reporter.newMetricBackendManager(),
                new HashMap<String, List<Backend>>(), new ArrayList<Backend>(),
                MetricBackendManager.DEFAULT_UPDATE_METADATA,
                MetricBackendManager.DEFAULT_GROUP_LIMIT,
                MetricBackendManager.DEFAULT_GROUP_LOAD_LIMIT,
                MetricBackendManager.DEFAULT_FLUSHING_INTERVAL);

        final UUID id = UUID.randomUUID();

        final NodeRegistryEntry localEntry = ClusterManagerImpl
                .buildLocalEntry(metrics, id, null, null);

        final ClusterManager cluster = new ClusterManagerImpl(
                ClusterDiscovery.NULL, id, null, null, localEntry);

        final MetadataBackendManager metadata = new MetadataBackendManager(
                reporter.newMetadataBackendManager(),
                new ArrayList<MetadataBackend>());

        final List<Consumer> consumers = new ArrayList<Consumer>();

        return new HeroicConfig(cluster, metrics, metadata, consumers, cache,
                DEFAULT_PORT, DEFAULT_REFRESH_CLUSTER_SCHEDULE);
    }

    public static HeroicConfig parse(Path path, HeroicReporter reporter)
            throws ValidationException, IOException {
        final Yaml yaml = new Yaml(new CustomConstructor());

        final HeroicConfigYAML configYaml = yaml.loadAs(
                Files.newInputStream(path), HeroicConfigYAML.class);

        if (configYaml == null)
            return HeroicConfig.buildDefault(reporter);

        return configYaml.build(reporter);
    }
}
