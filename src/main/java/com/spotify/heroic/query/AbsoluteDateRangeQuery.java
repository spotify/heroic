package com.spotify.heroic.query;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.spotify.heroic.model.DateRange;

@ToString(of = { "start", "end" })
public class AbsoluteDateRangeQuery implements DateRangeQuery {
    @Getter
    @Setter
    private long start;

    @Getter
    @Setter
    private long end;

    public DateRange buildDateRange() {
        return new DateRange(start, end);
    }
}