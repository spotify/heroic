package com.spotify.heroic.aggregation.simple;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.model.TimeData;

/**
 * Bucket that counts the number of seen events.
 * 
 * @author udoprog
 */
@Data
public class CountUniqueBucket implements Bucket<TimeData> {
    private final long timestamp;
    private final AtomicInteger count = new AtomicInteger(0);
    private final Set<Integer> seen = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(Map<String, String> tags, TimeData d) {
        if (seen.add(tags.hashCode() ^ d.hash()))
            count.incrementAndGet();
    }

    public long count() {
        return count.get();
    }
}