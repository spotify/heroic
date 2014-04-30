package com.spotify.heroic.query;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.spotify.heroic.model.DateRange;

@ToString(of = { "now", "unit", "value" })
public class RelativeDateRangeQuery implements DateRangeQuery {
    private final Date now = new Date();

    @Getter
    @Setter
    private TimeUnit unit = TimeUnit.DAYS;

    @Getter
    @Setter
    private long value = 1;

    public RelativeDateRangeQuery() {
    }

    public RelativeDateRangeQuery(TimeUnit unit, long value) {
        this.unit = unit;
        this.value = value;
    }

    private long start() {
        return now.getTime()
                - TimeUnit.MILLISECONDS.convert(value, unit);
    }

    private long end() {
        return now.getTime();
    }

    public DateRange buildDateRange() {
        return new DateRange(start(), end());
    }
}
