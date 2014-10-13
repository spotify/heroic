package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;

@RequiredArgsConstructor
public class MetadataManagerModule extends PrivateModule {
    private static final List<MetadataModule> DEFAULT_BACKENDS = new ArrayList<>();

    private final List<MetadataModule> backends;

    @JsonCreator
    public static MetadataManagerModule create(@JsonProperty("backends") List<MetadataModule> backends) {
        if (backends == null) {
            backends = DEFAULT_BACKENDS;
        }

        return new MetadataManagerModule(backends);
    }

    public static MetadataManagerModule createDefault() {
        return create(null);
    }

    @Provides
    @Singleton
    public LocalMetadataManagerReporter localReporter(HeroicReporter reporter) {
        return reporter.newLocalMetadataBackendManager();
    }

    @Provides
    @Singleton
    public ClusteredMetadataManagerReporter clusteredReporter(HeroicReporter reporter) {
        return reporter.newClusteredMetadataBackendManager();
    }

    @Override
    protected void configure() {
        bindBackends(backends);

        bind(MetadataManager.class).to(LocalMetadataManager.class).in(Scopes.SINGLETON);
        expose(MetadataManager.class);

        bind(ClusteredMetadataManager.class).in(Scopes.SINGLETON);
        expose(ClusteredMetadataManager.class);
    }

    private void bindBackends(final Collection<MetadataModule> configs) {
        final Multibinder<MetadataBackend> bindings = Multibinder.newSetBinder(binder(), MetadataBackend.class,
                Names.named("backends"));

        int i = 0;

        for (final MetadataModule config : configs) {
            final String id = config.id() != null ? config.id() : config.buildId(i++);

            final Key<MetadataBackend> key = Key.get(MetadataBackend.class, Names.named(id));

            install(config.module(key, id));

            bindings.addBinding().to(key);
        }
    }
}
