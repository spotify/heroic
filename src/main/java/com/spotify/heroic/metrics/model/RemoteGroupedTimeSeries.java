package com.spotify.heroic.metrics.model;

import java.util.Set;

import lombok.Data;

import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.model.TimeSerie;

@Data
public class RemoteGroupedTimeSeries {
    private final TimeSerie key;
    private final Set<TimeSerie> series;
    private final ClusterNode node;
}