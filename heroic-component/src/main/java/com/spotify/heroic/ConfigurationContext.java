package com.spotify.heroic;

public interface ConfigurationContext {
    void registerType(String name, Class<?> type);
}
