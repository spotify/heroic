package com.spotify.heroic.metadata.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;

@Data
public class TimeSerieQuery {
    private final String matchKey;
    private final Map<String, String> matchTags;
    private final Set<String> hasTags;
}
