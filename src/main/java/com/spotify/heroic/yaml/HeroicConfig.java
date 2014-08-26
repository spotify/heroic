package com.spotify.heroic.yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;
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
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.heroic.HeroicBackend;
import com.spotify.heroic.metrics.kairosdb.KairosBackend;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final long MAX_AGGREGATION_MAGNITUDE = 300000;
    public static final boolean UPDATE_METADATA = false;
    public static final long MAX_QUERIABLE_DATA_POINTS = 100000;
    public static final int DEFAULT_PORT = 8080;
    public static final String DEFAULT_REFRESH_CLUSTER_SCHEDULE = "0 */5 * * * ?";
    public static final int DEFAULT_GROUP_LIMIT = 500;
    public static final int DEFAULT_GROUP_LOAD_LIMIT = 5000;

    private final ClusterManager cluster;
    private final List<Backend> metricBackends;
    private final List<MetadataBackend> metadataBackends;
    private final List<Consumer> consumers;
    private final AggregationCache aggregationCache;
    private final boolean updateMetadata;
    private final int port;
    private final String refreshClusterSchedule;
    private final int groupLimit;
    private final int groupLoadLimit;

    private static final TypeDescription[] TYPES = new TypeDescription[] {
        Utils.makeType(HeroicBackend.YAML.class),
        Utils.makeType(KairosBackend.YAML.class),
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
        final ClusterManager cluster = ClusterManager.NULL;
        final List<Backend> metricBackends = new ArrayList<Backend>();
        final List<MetadataBackend> metadataBackends = new ArrayList<MetadataBackend>();
        final List<Consumer> consumers = new ArrayList<Consumer>();
        return new HeroicConfig(cluster, metricBackends, metadataBackends,
                consumers, cache, UPDATE_METADATA, DEFAULT_PORT,
                DEFAULT_REFRESH_CLUSTER_SCHEDULE, DEFAULT_GROUP_LIMIT,
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
