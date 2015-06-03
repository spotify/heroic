package com.spotify.heroic;

import java.util.List;

import com.google.inject.Module;

public interface HeroicConfigurationContext {
    /**
     * Register a new configuration type.
     */
    void registerType(String name, Class<?> type);

    void resource(Class<?> resource);

    void module(Module module);

    List<Class<?>> getResources();

    List<Module> getModules();
}
