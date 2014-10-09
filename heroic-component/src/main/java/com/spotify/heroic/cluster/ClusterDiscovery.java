package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.Collection;

import com.spotify.heroic.async.Callback;

public interface ClusterDiscovery {
    Callback<Collection<URI>> find();
}
