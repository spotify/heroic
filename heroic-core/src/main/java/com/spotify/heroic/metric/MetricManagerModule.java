package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetricManagerReporter;
import com.spotify.heroic.utils.BackendGroups;

@RequiredArgsConstructor
public class MetricManagerModule extends PrivateModule {
    public static final boolean DEFAULT_UPDATE_METADATA = false;
    public static final int DEFAULT_GROUP_LIMIT = 500;
    public static final int DEFAULT_GROUP_LOAD_LIMIT = 5000;
    public static final long DEFAULT_FLUSHING_INTERVAL = 1000;

    private final List<MetricModule> backends;
    private final List<String> defaultBackends;
    private final int groupLimit;
    private final int groupLoadLimit;
    private final long flushingInterval;

    @JsonCreator
    public static MetricManagerModule create(@JsonProperty("backends") List<MetricModule> backends,
            @JsonProperty("defaultBackends") List<String> defaultBackends,
            @JsonProperty("groupLimit") Integer groupLimit, @JsonProperty("groupLoadLimit") Integer groupLoadLimit,
            @JsonProperty("flushingInterval") Long flushingInterval) {
        if (backends == null)
            backends = new ArrayList<>();

        if (groupLimit == null)
            groupLimit = DEFAULT_GROUP_LIMIT;

        if (groupLoadLimit == null)
            groupLoadLimit = DEFAULT_GROUP_LOAD_LIMIT;

        if (flushingInterval == null)
            flushingInterval = DEFAULT_FLUSHING_INTERVAL;

        return new MetricManagerModule(backends, defaultBackends, groupLimit, groupLoadLimit, flushingInterval);
    }

    public static MetricManagerModule createDefault() {
        return create(null, null, null, null, null);
    }

    @Inject
    @Provides
    @Singleton
    public MetricManagerReporter reporter(HeroicReporter reporter) {
        return reporter.newMetricBackendManager();
    }

    @Provides
    @Named("backends")
    public BackendGroups<MetricBackend> defaultBackends(Set<MetricBackend> configured) {
        return BackendGroups.build(configured, defaultBackends);
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

    @Override
    protected void configure() {
        bindBackends(backends);
        bind(MetricManager.class).to(LocalMetricManager.class).in(Scopes.SINGLETON);
        expose(MetricManager.class);
        bind(ClusteredMetricManager.class).in(Scopes.SINGLETON);
        expose(ClusteredMetricManager.class);
    }

    private void bindBackends(final Collection<MetricModule> configs) {
        final Multibinder<MetricBackend> bindings = Multibinder.newSetBinder(binder(), MetricBackend.class);

        int i = 0;

        for (final MetricModule config : configs) {
            final String id = config.id() != null ? config.id() : config.buildId(i++);

            final Key<MetricBackend> key = Key.get(MetricBackend.class, Names.named(id));

            install(config.module(key, id));

            bindings.addBinding().to(key);
        }
    }
}
