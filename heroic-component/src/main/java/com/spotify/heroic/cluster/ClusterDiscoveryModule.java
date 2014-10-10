package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface ClusterDiscoveryModule {
    public Module module(final Key<ClusterDiscovery> key);

    public static class Null implements ClusterDiscovery {
        private static Null instance = new Null();

        @Override
        public Future<Collection<URI>> find() {
            return Futures.<Collection<URI>> resolved(new ArrayList<URI>());
        }

        public static ClusterDiscoveryModule module() {
            return new ClusterDiscoveryModule() {
                @Override
                public Module module(Key<ClusterDiscovery> key) {
                    return new PrivateModule() {
                        @Override
                        protected void configure() {
                            bind(ClusterDiscovery.class).toInstance(instance);
                            expose(ClusterDiscovery.class);
                        }
                    };
                }
            };
        }
    }
}
