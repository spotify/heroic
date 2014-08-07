package com.spotify.heroic.cluster;

import java.util.Collection;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.yaml.ValidationException;

public interface ClusterDiscovery {
    public interface YAML {
        public ClusterDiscovery build(String context) throws ValidationException;
    }

    Callback<Collection<ClusterNode>> getNodes();
}
