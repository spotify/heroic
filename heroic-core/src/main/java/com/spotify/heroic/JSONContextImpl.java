package com.spotify.heroic;

import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;

public class JSONContextImpl implements JSONContext {
    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public void registerType(String name, Class<?> type) {
        mapper.registerSubtypes(new NamedType(type, name));
    }
}
