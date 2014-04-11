package com.spotify.heroic.query;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = { "now", "unit", "value" })
public class RelativeDateRange implements DateRange {
    private final Date now = new Date();

    @Getter
    @Setter
    private TimeUnit unit = TimeUnit.DAYS;

    @Getter
    @Setter
    private long value = 1;

    @Override
    public long start() {
        return now.getTime()
                - TimeUnit.MILLISECONDS.convert(value, unit);
    }

    @Override
    public long end() {
        return now.getTime();
    }

    @Override
    public long diff() {
        return end() - start();
    }

    public RelativeDateRange() {
    }

    public RelativeDateRange(TimeUnit unit, long value) {
        this.unit = unit;
        this.value = value;
    }

    @Override
    public DateRange roundToInterval(long hint) {
        final AbsoluteDateRange range = new AbsoluteDateRange(start(), end());
        return range.roundToInterval(hint);
    }
}
