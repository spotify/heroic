package com.spotify.heroic.model;

import java.util.concurrent.TimeUnit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = { "unit", "value" })
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
