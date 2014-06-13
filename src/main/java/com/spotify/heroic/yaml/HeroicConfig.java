package com.spotify.heroic.yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import lombok.Getter;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.EventBackend;
import com.spotify.heroic.backend.ListBackendManager;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.kairosdb.KairosDBBackend;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.InMemoryAggregationCacheBackend;
import com.spotify.heroic.cache.cassandra.CassandraCache;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class HeroicConfig {
    public static final long MAX_AGGREGATION_MAGNITUDE = 300000;
    public static final long MAX_QUERIABLE_DATA_POINTS = 100000;

    @Getter
    private final BackendManager backendManager;

    @Getter
    private final AggregationCache cache;

    public HeroicConfig(BackendManager backendManager, AggregationCache cache) {
        this.backendManager = backendManager;
        this.cache = cache;
    }

    private static final TypeDescription[] TYPES = new TypeDescription[] {
            Utils.makeType(KairosDBBackend.YAML.class),
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
                cache, reporter.newBackendManager(null), MAX_AGGREGATION_MAGNITUDE);
        return new HeroicConfig(backendManager, null);
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
