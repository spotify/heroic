package com.spotify.heroic.metadata;

import com.google.inject.Key;
import com.google.inject.Module;

public interface MetadataModule {
    public String buildId(int i);

    public String id();

    public Module module(Key<MetadataBackend> key, String id);
}
