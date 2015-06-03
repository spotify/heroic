package com.spotify.heroic.elasticsearch;

import org.elasticsearch.client.Client;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = StandaloneClientSetup.class, name = "standalone"),
        @JsonSubTypes.Type(value = NodeClientSetup.class, name = "node"),
        @JsonSubTypes.Type(value = TransportClientSetup.class, name = "transport") })
public interface ClientSetup {
    public Client setup() throws Exception;

    public void stop() throws Exception;
}