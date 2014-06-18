package com.spotify.heroic.aggregation;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.model.Resolution;

@ToString(of={"sampling"})
@EqualsAndHashCode(of={"sampling"})
public abstract class SumBucketAggregation implements Aggregation {
    @Getter
    private final Resolution sampling;

    public long getWidth() {
        return sampling.getWidth();
    }

    public SumBucketAggregation(Resolution sampling) {
        this.sampling = sampling;
    }
}