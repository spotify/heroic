package com.spotify.heroic.cluster.discovery.simple;

import java.net.URI;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.ClusterDiscovery;

@RequiredArgsConstructor
public class StaticListDiscovery implements ClusterDiscovery {
    @Inject
    @Named("nodes")
    private List<URI> nodes;

    @Override
    public Callback<Collection<URI>> find() {
        return new ResolvedCallback<Collection<URI>>(nodes);
    }
}
