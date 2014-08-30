package com.spotify.heroic.cluster;

import java.util.Collection;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ValidationException;

public interface ClusterDiscovery {
    public interface YAML {
        public ClusterDiscovery build(ConfigContext context)
                throws ValidationException;
    }

    public static final class Null implements ClusterDiscovery {
        private Null() {
        }

        @Override
        public Callback<Collection<DiscoveredClusterNode>> getNodes() {
            throw new NullPointerException();
        }
    }

    public static final ClusterDiscovery NULL = new Null();

    Callback<Collection<DiscoveredClusterNode>> getNodes();
}
