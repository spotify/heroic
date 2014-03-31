package com.spotify.heroic.query;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

public class RelativeDateRange implements DateRange {
    private final Date now = new Date();

    @Getter
    @Setter
    private TimeUnit unit;

    @Getter
    @Setter
    private long value;

    @Override
    public Date start() {
        final long start = now.getTime()
                - TimeUnit.MILLISECONDS.convert(value, unit);
        return new Date(start);
    }

    @Override
    public Date end() {
        return now;
    }

    public RelativeDateRange() {
    }

    public RelativeDateRange(TimeUnit unit, long value) {
        this.unit = unit;
        this.value = value;
    }
}
