package com.spotify.heroic.metadata.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.Series;

@Data
public class ScoredSeries {
    private final float score;
    private final Series series;

    @JsonCreator
    public static ScoredSeries create(@JsonProperty("score") Float score, @JsonProperty("series") Series series) {
        return new ScoredSeries(score, series);
    }
}
