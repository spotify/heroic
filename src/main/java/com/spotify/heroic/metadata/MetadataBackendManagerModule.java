package com.spotify.heroic.metadata;

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
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;

@RequiredArgsConstructor
public class MetadataBackendManagerModule extends PrivateModule {
    private final List<MetadataBackendConfig> backends;

    @JsonCreator
    public static MetadataBackendManagerModule create(
            @JsonProperty("backends") List<MetadataBackendConfig> backends) {
        return new MetadataBackendManagerModule(backends);
    }

    @Provides
    @Singleton
    public MetadataBackendManagerReporter reporter(HeroicReporter reporter) {
        return reporter.newMetadataBackendManager();
    }

    @Override
    protected void configure() {
        bindBackends(backends);
        bind(MetadataBackendManager.class).in(Scopes.SINGLETON);
        expose(MetadataBackendManager.class);
    }

    private void bindBackends(final Collection<MetadataBackendConfig> configs) {
        final Multibinder<MetadataBackend> bindings = Multibinder.newSetBinder(
                binder(), MetadataBackend.class, Names.named("backends"));

        int i = 0;

        for (final MetadataBackendConfig config : configs) {
            final String id = config.id() != null ? config.id() : config
                    .buildId(i++);

            final Key<MetadataBackend> key = Key.get(MetadataBackend.class,
                    Names.named(id));

            install(config.module(key, id));

            bindings.addBinding().to(key);
        }
    }
}
