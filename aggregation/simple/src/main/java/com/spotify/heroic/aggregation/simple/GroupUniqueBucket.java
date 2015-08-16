package com.spotify.heroic.aggregation.simple;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypedGroup;

@RequiredArgsConstructor
public class GroupUniqueBucket implements Bucket<Metric> {
    final Set<Member> groups = Collections.newSetFromMap(new ConcurrentHashMap<Member, Boolean>());

    final long timestamp;

    public List<MetricTypedGroup> groups() {
        final ImmutableList.Builder<Metric> pointsBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Metric> eventsBuilder = ImmutableList.builder();

        for (final Member m : groups) {
            switch (m.type) {
            case POINT:
                pointsBuilder.add(m.sample);
                break;
            case EVENT:
                eventsBuilder.add(m.sample);
                break;
            default:
                break;
            }
        }

        final List<Metric> points = pointsBuilder.build();
        final List<Metric> events = eventsBuilder.build();

        final ImmutableList.Builder<MetricTypedGroup> result = ImmutableList.builder();

        if (!points.isEmpty()) {
            result.add(new MetricTypedGroup(MetricType.POINT, points));
        }

        if (!events.isEmpty()) {
            result.add(new MetricTypedGroup(MetricType.EVENT, events));
        }

        return result.build();
    }

    @Override
    public void update(Map<String, String> tags, MetricType type, Metric sample) {
        groups.add(new Member(sample.valueHash(), type, sample));
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Data
    @EqualsAndHashCode(of = { "hash", "type" })
    static class Member {
        final int hash;
        final MetricType type;
        final Metric sample;
    }
}