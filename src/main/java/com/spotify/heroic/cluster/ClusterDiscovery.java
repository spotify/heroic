package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.discovery.StaticListDiscovery;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = StaticListDiscovery.class, name = "static") })
public interface ClusterDiscovery {
    public static final class Null implements ClusterDiscovery {
        private Null() {
        }

        @Override
        public Callback<Collection<URI>> find() {
            throw new NullPointerException();
        }
    }

    public static final ClusterDiscovery NULL = new Null();

    Callback<Collection<URI>> find();
}
