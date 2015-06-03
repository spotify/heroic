package com.spotify.heroic.metadata;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.utils.BackendGroups;

public class MetadataManagerModule extends PrivateModule {
    private static final List<MetadataModule> DEFAULT_BACKENDS = ImmutableList.of();

    private final List<MetadataModule> backends;
    private final List<String> defaultBackends;

    @JsonCreator
    public MetadataManagerModule(@JsonProperty("backends") List<MetadataModule> backends,
            @JsonProperty("defaultBackends") List<String> defaultBackends) {
        this.backends = Optional.fromNullable(backends).or(DEFAULT_BACKENDS);
        this.defaultBackends = defaultBackends;
    }

    public static Supplier<MetadataManagerModule> defaultSupplier() {
        return new Supplier<MetadataManagerModule>() {
            @Override
            public MetadataManagerModule get() {
                return new MetadataManagerModule(null, null);
            }
        };
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

    @Provides
    @Named("backends")
    public BackendGroups<MetadataBackend> defaultBackends(Set<MetadataBackend> configured) {
        return BackendGroups.build(configured, defaultBackends);
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
        final Multibinder<MetadataBackend> bindings = Multibinder.newSetBinder(binder(), MetadataBackend.class);

        int i = 0;

        for (final MetadataModule config : configs) {
            final String id = config.id() != null ? config.id() : config.buildId(i++);

            final Key<MetadataBackend> key = Key.get(MetadataBackend.class, Names.named(id));

            install(config.module(key, id));

            bindings.addBinding().to(key);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<MetadataModule> backends;
        private List<String> defaultBackends;

        public Builder backends(List<MetadataModule> backends) {
            this.backends = backends;
            return this;
        }

        public Builder defaultBackends(List<String> defaultBackends) {
            this.defaultBackends = defaultBackends;
            return this;
        }

        public MetadataManagerModule build() {
            return new MetadataManagerModule(backends, defaultBackends);
        }
    }
}
