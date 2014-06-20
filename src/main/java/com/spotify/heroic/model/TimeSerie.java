package com.spotify.heroic.model;

import java.util.HashMap;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString(of = { "key", "tags" })
@EqualsAndHashCode(of = { "key", "tags" })
public class TimeSerie {
    @Getter
    private final String key;
    @Getter
    private final Map<String, String> tags;

    public TimeSerie(String key, Map<String, String> tags) {
        this.key = key;
        this.tags = tags;
    }

    public TimeSerieSlice slice(DateRange range) {
        return new TimeSerieSlice(this, range);
    }

    public TimeSerieSlice slice(long start, long end) {
        return new TimeSerieSlice(this, new DateRange(start, end));
    }

    public TimeSerie modifyTags(Map<String, String> tags) {
        final Map<String, String> modifiedTags = new HashMap<String, String>(
                this.tags);
        modifiedTags.putAll(tags);
        return new TimeSerie(key, modifiedTags);
    }

    public TimeSerie withTags(Map<String, String> tags) {
        return new TimeSerie(key, tags);
    }
}
