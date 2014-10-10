package com.spotify.heroic.aggregation;

import com.spotify.heroic.model.DataPoint;

public interface Bucket {
    public void update(DataPoint d);

    public long timestamp();
}
