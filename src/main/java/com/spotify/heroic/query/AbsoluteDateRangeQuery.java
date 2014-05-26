package com.spotify.heroic.query;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DateRange;

@ToString(of = { "start", "end" })
@RequiredArgsConstructor
public class AbsoluteDateRangeQuery implements DateRangeQuery {
    @Getter
    private final long start;

    @Getter
    private final long end;

    @JsonCreator
    public static AbsoluteDateRangeQuery create(
            @JsonProperty(value="start", required=true) long start,
            @JsonProperty(value="end", required=true) long end) {
        return new AbsoluteDateRangeQuery(start, end);
    }

    public DateRange buildDateRange() {
        return new DateRange(start, end);
    }
}