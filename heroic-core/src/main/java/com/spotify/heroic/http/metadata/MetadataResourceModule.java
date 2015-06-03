package com.spotify.heroic.http.metadata;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

public class MetadataResourceModule extends PrivateModule {
    @Override
    protected void configure() {
        bind(MetadataResourceCache.class);
        bind(MetadataResource.class).in(Scopes.SINGLETON);
        expose(MetadataResource.class);
    }
}