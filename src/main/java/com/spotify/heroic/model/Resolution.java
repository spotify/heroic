package com.spotify.heroic.model;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.ToString;

@ToString(of = { "unit", "value" })
public class Resolution {
    public static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;
    public static final long DEFAULT_VALUE = 10;

    public static final Resolution DEFAULT_RESOLUTION = new Resolution(
            DEFAULT_UNIT, 5);

    @Getter
    private final TimeUnit unit;

    @Getter
    private final long value;

    public Resolution(TimeUnit unit, long value) {
        this.unit = unit;
        this.value = value;
    }

    @JsonCreator
    public static Resolution create(@JsonProperty("unit") TimeUnit unit, @JsonProperty("value") Long value) {
        if (unit == null)
            unit = DEFAULT_UNIT;

        if (value == null)
            value = DEFAULT_VALUE;

        return new Resolution(unit, value);
    }

    public long getWidth() {
        return TimeUnit.MILLISECONDS.convert(value, unit);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (getWidth() ^ (getWidth() >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        if (getClass() != obj.getClass())
            return false;

        final Resolution other = (Resolution) obj;

        if (getWidth() != other.getWidth())
            return false;

        return true;
    }
}
