package com.spotify.heroic.aggregation;

import java.util.Map;

public interface Bucket<T> {
    public void update(Map<String, String> tags, T d);

    public long timestamp();
}