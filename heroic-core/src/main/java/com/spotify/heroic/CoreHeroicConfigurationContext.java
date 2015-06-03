package com.spotify.heroic;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;

public class CoreHeroicConfigurationContext implements HeroicConfigurationContext {
    @Inject
    @Named("application/heroic-config")
    private ObjectMapper mapper;

    private final List<Class<?>> resources = new ArrayList<>();
    private final List<Module> modules = new ArrayList<>();

    @Override
    public void registerType(String name, Class<?> type) {
        mapper.registerSubtypes(new NamedType(type, name));
    }

    @Override
    public void resource(Class<?> type) {
        resources.add(type);
    }

    @Override
    public void module(Module module) {
        modules.add(module);
    }

    @Override
    public List<Class<?>> getResources() {
        return ImmutableList.copyOf(resources);
    }

    @Override
    public List<Module> getModules() {
        return ImmutableList.copyOf(modules);
    }
}
