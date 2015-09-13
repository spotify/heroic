package com.spotify.heroic.aggregation;

import java.util.Map;

import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

public abstract class AbstractBucket implements Bucket {
    @Override
    public void updatePoint(Map<String, String> tags, Point sample) {
    }

    @Override
    public void updateEvent(Map<String, String> tags, Event sample) {
    }

    @Override
    public void updateSpread(Map<String, String> tags, Spread sample) {
    }

    @Override
    public void updateGroup(Map<String, String> tags, MetricGroup sample) {
    }
}