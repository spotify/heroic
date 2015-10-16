package com.spotify.heroic.shell.task;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricType;

import lombok.Data;

@Data
public class BackendKeyArgument {
    private final Series series;
    private final long base;
    private final MetricType type;

    @JsonCreator
    public BackendKeyArgument(@JsonProperty("series") Series series, @JsonProperty("base") Long base,
            @JsonProperty("type") MetricType type) {
        this.series = checkNotNull(series, "series");
        this.base = checkNotNull(base, "base");
        this.type = Optional.ofNullable(type).orElse(MetricType.POINT);
    }

    public BackendKey toBackendKey() {
        return new BackendKey(series, base, type);
    }
}