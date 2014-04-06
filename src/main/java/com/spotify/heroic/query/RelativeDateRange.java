package com.spotify.heroic.query;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

public class RelativeDateRange implements DateRange {
    private final Date now = new Date();

    @Getter
    @Setter
    private TimeUnit unit = TimeUnit.DAYS;

    @Getter
    @Setter
    private long value = 1;

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

    @Override
    public DateRange roundToInterval(long hint) {
        final AbsoluteDateRange range = new AbsoluteDateRange(
                start().getTime(), end().getTime());
        return range.roundToInterval(hint);
    }
}
