package com.spotify.heroic.metadata;

import java.util.Map;
import java.util.Set;

import com.spotify.heroic.model.TimeSerie;

public interface TimeSerieMatcher {
    public boolean matches(TimeSerie timeserie);

    public String matchKey();

    public Map<String, String> matchTags();

    public Set<String> hasTags();
}