package com.spotify.heroic.yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import lombok.Getter;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.codahale.metrics.MetricRegistry;
import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.ListBackendManager;
import com.spotify.heroic.backend.kairosdb.KairosDBBackend;

public class HeroicConfig {
    public static final long DEFAULT_TIMEOUT = 10000;
    public static final long MAX_AGGREGATION_MAGNITUDE = 300000;
    public static final long MAX_QUERIABLE_DATA_POINTS = 100000;

    @Getter
    private final BackendManager backendManager;

    public HeroicConfig(BackendManager backendManager) {
        this.backendManager = backendManager;
    }

    private static final TypeDescription[] types = new TypeDescription[] { Utils
            .makeType(KairosDBBackend.YAML.class) };

    private static final class CustomConstructor extends Constructor {
        public CustomConstructor() {
            for (final TypeDescription t : types) {
                addTypeDescription(t);
            }
        }
    }

    public static HeroicConfig buildDefault(MetricRegistry registry) {
        final BackendManager backendManager = new ListBackendManager(
                new ArrayList<Backend>(), registry, DEFAULT_TIMEOUT,
                MAX_AGGREGATION_MAGNITUDE, MAX_QUERIABLE_DATA_POINTS);
        return new HeroicConfig(backendManager);
    }

    public static HeroicConfig parse(Path path, MetricRegistry registry)
            throws ValidationException, IOException {
        final Yaml yaml = new Yaml(new CustomConstructor());

        final HeroicConfigYAML configYaml = yaml.loadAs(
                Files.newInputStream(path), HeroicConfigYAML.class);

        if (configYaml == null) {
            return HeroicConfig.buildDefault(registry);
        }

        return configYaml.build(registry);
    }
}
