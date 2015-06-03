package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.List;

import eu.toolchain.async.AsyncFuture;

public interface ClusterDiscovery {
    AsyncFuture<List<URI>> find();
}