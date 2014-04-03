package com.spotify.heroic.yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import lombok.Getter;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.ListBackendManager;
import com.spotify.heroic.backend.kairosdb.KairosDBBackend;

public class HeroicConfig {
    public static final long DEFAULT_TIMEOUT = 10000;

    @Getter
    private final BackendManager backendManager;

    public HeroicConfig(BackendManager backendManager) {
        this.backendManager = backendManager;
    }

    private static final TypeDescription[] types = new TypeDescription[] { Utils
            .makeType(KairosDBBackend.YAML.class) };

    private static final class CustomConstructor extends Constructor {
        public CustomConstructor() {
            for (TypeDescription t : types) {
                addTypeDescription(t);
            }
        }
    }

    public static HeroicConfig buildDefault() {
        final BackendManager backendManager = new ListBackendManager(
                new ArrayList<Backend>(), DEFAULT_TIMEOUT);
        return new HeroicConfig(backendManager);
    }

    public static HeroicConfig parse(Path path) throws ValidationException,
            IOException {
        final Yaml yaml = new Yaml(new CustomConstructor());

        final HeroicConfigYAML configYaml = yaml.loadAs(
                Files.newInputStream(path), HeroicConfigYAML.class);

        if (configYaml == null) {
            return HeroicConfig.buildDefault();
        }

        return configYaml.build();
    }
}
