package com.spotify.heroic.model;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class Series {
    private final String key;
    private final Map<String, String> tags;

    public SeriesSlice slice(DateRange range) {
        return new SeriesSlice(this, range);
    }

    public SeriesSlice slice(long start, long end) {
        return new SeriesSlice(this, new DateRange(start, end));
    }

    public Series modifyTags(Map<String, String> tags) {
        final Map<String, String> modifiedTags = new HashMap<String, String>(
                this.tags);
        modifiedTags.putAll(tags);
        return new Series(key, modifiedTags);
    }

    public Series withTags(Map<String, String> tags) {
        return new Series(key, tags);
    }

    @JsonCreator
    public static Series create(
            @JsonProperty(value = "key", required = true) String key,
            @JsonProperty(value = "tags", required = true) Map<String, String> tags) {
        return new Series(key, tags);
    }
}
