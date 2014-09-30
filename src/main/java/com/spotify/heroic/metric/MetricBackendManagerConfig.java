package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@Data
public class MetricBackendManagerConfig {
    public static final boolean DEFAULT_UPDATE_METADATA = false;
    public static final int DEFAULT_GROUP_LIMIT = 500;
    public static final int DEFAULT_GROUP_LOAD_LIMIT = 5000;
    public static final long DEFAULT_FLUSHING_INTERVAL = 1000;

    private final List<MetricBackendConfig> backends;
    private final List<String> defaultBackends;
    private final boolean updateMetadata;
    private final int groupLimit;
    private final int groupLoadLimit;
    private final long flushingInterval;

    @JsonCreator
    public static MetricBackendManagerConfig create(
            @JsonProperty("backends") List<MetricBackendConfig> backends,
            @JsonProperty("defaultBackends") List<String> defaultBackends,
            @JsonProperty("updateMetadata") Boolean updateMetadata,
            @JsonProperty("groupLimit") Integer groupLimit,
            @JsonProperty("groupLoadLimit") Integer groupLoadLimit,
            @JsonProperty("flushingInterval") Long flushingInterval) {
        if (backends == null)
            backends = new ArrayList<>();

        if (updateMetadata == null)
            updateMetadata = DEFAULT_UPDATE_METADATA;

        if (groupLimit == null)
            groupLimit = DEFAULT_GROUP_LIMIT;

        if (groupLoadLimit == null)
            groupLoadLimit = DEFAULT_GROUP_LOAD_LIMIT;

        if (flushingInterval == null)
            flushingInterval = DEFAULT_FLUSHING_INTERVAL;

        return new MetricBackendManagerConfig(backends, defaultBackends,
                updateMetadata, groupLimit, groupLoadLimit, flushingInterval);
    }

    public Module module() {
        return new PrivateModule() {
            @Inject
            @Provides
            @Singleton
            public MetricBackendManagerReporter reporter(HeroicReporter reporter) {
                return reporter.newMetricBackendManager();
            }

            @Provides
            @Named("backends")
            public Map<String, List<MetricBackend>> buildBackends(
                    Set<MetricBackend> backends) {
                final Map<String, List<MetricBackend>> groups = new HashMap<>();

                for (final MetricBackend backend : backends) {
                    List<MetricBackend> group = groups.get(backend.getGroup());

                    if (group == null) {
                        group = new ArrayList<>();
                        groups.put(backend.getGroup(), group);
                    }

                    group.add(backend);
                }

                return groups;
            }

            @Provides
            @Named("defaultBackends")
            public List<MetricBackend> defaultBackends(
                    @Named("backends") Map<String, List<MetricBackend>> backends) {
                if (defaultBackends == null) {
                    final List<MetricBackend> result = new ArrayList<>();

                    for (final Map.Entry<String, List<MetricBackend>> entry : backends
                            .entrySet()) {
                        result.addAll(entry.getValue());
                    }

                    return result;
                }

                final List<MetricBackend> result = new ArrayList<>();

                for (final String defaultBackend : defaultBackends) {
                    final List<MetricBackend> someResult = backends
                            .get(defaultBackend);

                    if (someResult == null)
                        throw new IllegalArgumentException(
                                "No backend(s) available with id : "
                                        + defaultBackend);

                    result.addAll(someResult);
                }

                return result;
            }

            @Provides
            @Named("flushingInterval")
            public long flushingInterval() {
                return flushingInterval;
            }

            @Provides
            @Named("groupLimit")
            public int groupLimit() {
                return groupLimit;
            }

            @Provides
            @Named("groupLoadLimit")
            public int groupLoadLimit() {
                return groupLoadLimit;
            }

            @Provides
            @Named("updateMetadata")
            public boolean updateMetadata() {
                return updateMetadata;
            }

            @Override
            protected void configure() {
                bindBackends(backends);
                bind(MetricBackendManager.class).in(Scopes.SINGLETON);
                expose(MetricBackendManager.class);
            }

            private void bindBackends(
                    final Collection<MetricBackendConfig> configs) {
                final Multibinder<MetricBackend> bindings = Multibinder
                        .newSetBinder(binder(), MetricBackend.class);

                int i = 0;

                for (final MetricBackendConfig config : configs) {
                    final String id = config.id() != null ? config.id()
                            : config.buildId(i++);

                    final Key<MetricBackend> key = Key.get(MetricBackend.class,
                            Names.named(id));

                    install(config.module(key, id));

                    bindings.addBinding().to(key);
                }
            }
        };
    }
}
