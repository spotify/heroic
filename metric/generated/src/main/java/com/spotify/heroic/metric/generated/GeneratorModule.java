package com.spotify.heroic.metric.generated;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Module;
import com.spotify.heroic.metric.generated.generator.RandomGeneratorModule;
import com.spotify.heroic.metric.generated.generator.SineGeneratorModule;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = RandomGeneratorModule.class, name = "random"),
        @JsonSubTypes.Type(value = SineGeneratorModule.class, name = "sine") })
public interface GeneratorModule {
    public Module module();
}
