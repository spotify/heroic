package com.spotify.heroic.metrics.model;

import java.util.Set;

import lombok.Data;

import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.model.Series;

@Data
public class RemoteGroupedTimeSeries {
    private final Series key;
    private final Set<Series> series;
    private final ClusterNode node;
}