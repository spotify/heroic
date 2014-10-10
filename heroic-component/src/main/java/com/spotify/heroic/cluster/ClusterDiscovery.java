package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.Collection;

import com.spotify.heroic.async.Future;

public interface ClusterDiscovery {
    Future<Collection<URI>> find();
}
