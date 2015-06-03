package com.spotify.heroic.statistics;

import java.util.Map;
import java.util.Set;

public interface ClusteredManager {
    /**
     * Register known shards at the time that they become known.
     *
     * This method is used to 'prepare' the reporter for a specific set of shards.
     *
     * @param shards
     *            The shards that are known to exist.
     */
    void registerShards(Set<Map<String, String>> shards);
}