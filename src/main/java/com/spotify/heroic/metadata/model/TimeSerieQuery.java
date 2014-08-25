package com.spotify.heroic.metadata.model;

import lombok.Data;

import com.spotify.heroic.model.filter.Filter;

@Data
public class TimeSerieQuery {
    private final Filter filter;
}
