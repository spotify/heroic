package com.spotify.heroic.cluster.discovery.simple;

import java.net.URI;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.cluster.ClusterDiscovery;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@RequiredArgsConstructor
@ToString
public class StaticListDiscovery implements ClusterDiscovery {
    @Inject
    private AsyncFramework async;

    @Inject
    @Named("nodes")
    private List<URI> nodes;

    @Override
    public AsyncFuture<List<URI>> find() {
        return async.resolved(nodes);
    }
}
