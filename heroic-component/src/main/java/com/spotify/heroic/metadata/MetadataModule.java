package com.spotify.heroic.metadata;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Key;
import com.google.inject.Module;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface MetadataModule {
    public String buildId(int i);

    public String id();

    public Module module(Key<MetadataBackend> key, String id);
}
