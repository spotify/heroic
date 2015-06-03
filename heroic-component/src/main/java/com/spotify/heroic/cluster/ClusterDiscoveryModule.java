package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

public interface ClusterDiscoveryModule {
    public Module module(final Key<ClusterDiscovery> key);

    public static class Null implements ClusterDiscovery {
        @Inject
        private AsyncFramework async;

        @Override
        public AsyncFuture<List<URI>> find() {
            return async.<List<URI>> resolved(new ArrayList<URI>());
        }

        public static Supplier<ClusterDiscoveryModule> supplier() {
            return new Supplier<ClusterDiscoveryModule>() {
                @Override
                public ClusterDiscoveryModule get() {
                    return new ClusterDiscoveryModule() {
                        @Override
                        public Module module(Key<ClusterDiscovery> key) {
                            return new PrivateModule() {
                                @Override
                                protected void configure() {
                                    bind(ClusterDiscovery.class).to(Null.class);
                                    expose(ClusterDiscovery.class);
                                }
                            };
                        }
                    };
                }
            };
        }
    }
}
