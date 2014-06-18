package com.spotify.heroic.yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.list.ListBackendManager;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.InMemoryAggregationCacheBackend;
import com.spotify.heroic.cache.cassandra.CassandraCache;
import com.spotify.heroic.events.EventBackend;
import com.spotify.heroic.metadata.InMemoryMetadataBackend;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.kairosdb.KairosMetricBackend;
import com.spotify.heroic.statistics.HeroicReporter;

@RequiredArgsConstructor
public class HeroicConfig {
    public static final long MAX_AGGREGATION_MAGNITUDE = 300000;
    public static final long MAX_QUERIABLE_DATA_POINTS = 100000;

    @Getter
    private final BackendManager backendManager;

    @Getter
    private final AggregationCache aggregationCache;

    @Getter
    private final MetadataBackend metadataBackend;

    private static final TypeDescription[] TYPES = new TypeDescription[] {
            Utils.makeType(KairosMetricBackend.YAML.class),
            Utils.makeType(InMemoryAggregationCacheBackend.YAML.class),
            Utils.makeType(CassandraCache.YAML.class) };

    private static final class CustomConstructor extends Constructor {
        public CustomConstructor() {
            for (final TypeDescription t : TYPES) {
                addTypeDescription(t);
            }
        }
    }

    public static HeroicConfig buildDefault(HeroicReporter reporter) {
        final AggregationCache cache = new AggregationCache(reporter.newAggregationCache(null), new InMemoryAggregationCacheBackend());
        final BackendManager backendManager = new ListBackendManager(
                new ArrayList<MetricBackend>(), new ArrayList<EventBackend>(),
                reporter.newBackendManager(null), MAX_AGGREGATION_MAGNITUDE);
        final MetadataBackend metadataBackend = new InMemoryMetadataBackend();
        return new HeroicConfig(backendManager, cache, metadataBackend);
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
