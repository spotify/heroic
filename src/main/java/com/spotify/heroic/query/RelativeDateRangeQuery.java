package com.spotify.heroic.query;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DateRange;

@ToString(of = {"unit", "value"})
@EqualsAndHashCode(of={"unit", "value"})
@RequiredArgsConstructor
public class RelativeDateRangeQuery implements DateRangeQuery {
    public static final TimeUnit DEFAULT_UNIT = TimeUnit.DAYS;
    public static final long DEFAULT_VALUE = 1;

    @Getter
    private final TimeUnit unit;

    @Getter
    private final long value;

    @JsonCreator
    public static RelativeDateRangeQuery create(@JsonProperty("unit") TimeUnit unit, @JsonProperty("value") Long value) {
        if (unit == null)
            unit = DEFAULT_UNIT;

        if (value == null)
            value = DEFAULT_VALUE;

        return new RelativeDateRangeQuery(unit, value);
    }

    private long start(final Date now) {
        return now.getTime()
                - TimeUnit.MILLISECONDS.convert(value, unit);
    }

    private long end(final Date now) {
        return now.getTime();
    }

    public DateRange buildDateRange() {
        final Date now = new Date();
        return new DateRange(start(now), end(now));
    }
}
