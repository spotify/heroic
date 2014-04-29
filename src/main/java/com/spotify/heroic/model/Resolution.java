package com.spotify.heroic.model;

import java.util.concurrent.TimeUnit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = { "unit", "value" })
@EqualsAndHashCode(of = { "unit", "value" })
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
