package com.spotify.heroic.suggest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
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

public class SuggestManagerModule extends PrivateModule {
    private static final List<SuggestModule> DEFAULT_BACKENDS = new ArrayList<>();

    private final List<SuggestModule> backends;
    private final List<String> defaultBackends;

    @JsonCreator
    public SuggestManagerModule(@JsonProperty("backends") List<SuggestModule> backends,
            @JsonProperty("defaultBackends") List<String> defaultBackends) {
        this.backends = Optional.fromNullable(backends).or(DEFAULT_BACKENDS);
        this.defaultBackends = defaultBackends;
    }

    public static Supplier<SuggestManagerModule> defaultSupplier() {
        return new Supplier<SuggestManagerModule>() {
            @Override
            public SuggestManagerModule get() {
                return new SuggestManagerModule(null, null);
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
    public BackendGroups<SuggestBackend> defaultBackends(Set<SuggestBackend> configured) {
        return BackendGroups.build(configured, defaultBackends);
    }

    @Override
    protected void configure() {
        bindBackends(backends);

        bind(SuggestManager.class).to(LocalSuggestManager.class).in(Scopes.SINGLETON);
        expose(SuggestManager.class);

        bind(ClusteredSuggestManager.class).in(Scopes.SINGLETON);
        expose(ClusteredSuggestManager.class);
    }

    private void bindBackends(final Collection<SuggestModule> configs) {
        final Multibinder<SuggestBackend> bindings = Multibinder.newSetBinder(binder(), SuggestBackend.class);

        int i = 0;

        for (final SuggestModule config : configs) {
            final String id = config.id() != null ? config.id() : config.buildId(i++);

            final Key<SuggestBackend> key = Key.get(SuggestBackend.class, Names.named(id));

            install(config.module(key, id));

            bindings.addBinding().to(key);
        }
    }
}
