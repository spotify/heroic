package com.spotify.heroic.metric.datastax.schema;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Module;
import com.spotify.heroic.metric.datastax.schema.legacy.LegacySchemaModule;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchemaModule;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = LegacySchemaModule.class, name = "legacy"),
    @JsonSubTypes.Type(value = NextGenSchemaModule.class, name = "ng") })
public interface SchemaModule {
    public Module module();
}