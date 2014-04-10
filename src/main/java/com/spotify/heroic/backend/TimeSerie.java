package com.spotify.heroic.backend;

import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;

@ToString(of = { "key", "tags" })
@EqualsAndHashCode(of = { "key", "tags" })
public class TimeSerie {
    @Getter
    @JsonIgnore
    private final DataPointsRowKey rowKey;
    @Getter
    private final String key;
    @Getter
    private final Map<String, String> tags;

    public TimeSerie(DataPointsRowKey rowKey, String key,
            Map<String, String> tags) {
        this.rowKey = rowKey;
        this.key = key;
        this.tags = tags;
    }
}
