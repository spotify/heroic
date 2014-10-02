package com.spotify.heroic.ingestion;

import javax.inject.Named;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;

@RequiredArgsConstructor
public class IngestionModule extends PrivateModule {
    public static final boolean DEFAULT_UPDATE_METADATA = true;
    public static final boolean DEFAULT_UPDATE_METRICS = true;

    private final boolean updateMetadata;
    private final boolean updateMetrics;

    @Provides
    @Named("updateMetadata")
    public boolean updateMetadata() {
        return updateMetadata;
    }

    @Provides
    @Named("updateMetrics")
    public boolean updateMetrics() {
        return updateMetrics;
    }

    @Override
    protected void configure() {
        bind(IngestionManager.class).in(Scopes.SINGLETON);
        expose(IngestionManager.class);
    }

    @JsonCreator
    public static IngestionModule create(
            @JsonProperty("updateMetadata") Boolean updateMetadata,
            @JsonProperty("updateMetrics") Boolean updateMetrics) {
        if (updateMetadata == null) {
            updateMetadata = DEFAULT_UPDATE_METADATA;
        }

        if (updateMetrics == null) {
            updateMetrics = DEFAULT_UPDATE_METRICS;
        }

        return new IngestionModule(updateMetadata, updateMetrics);
    }

    public static IngestionModule createDefault() {
        return create(null, null);
    }
}
