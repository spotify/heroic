package com.spotify.heroic.query;

import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

public class Resolution {
    public static final Resolution DEFAULT_RESOLUTION = new Resolution(
            TimeUnit.MINUTES, 5);

    @Getter
    @Setter
    private TimeUnit unit = TimeUnit.MINUTES;

    @Getter
    @Setter
    private long value = 5;

    public Resolution() {
    }

    public Resolution(TimeUnit unit, long value) {
        this.unit = unit;
        this.value = value;
    }

    public long getWidth() {
        return TimeUnit.MILLISECONDS.convert(value, unit);
    }
}
