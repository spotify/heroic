package com.spotify.heroic.suggest;

import com.google.inject.Key;
import com.google.inject.Module;

public interface SuggestModule {
    public String buildId(int i);

    public String id();

    public Module module(Key<SuggestBackend> key, String id);
}