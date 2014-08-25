package com.spotify.heroic.http.model.query;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.model.DateRange;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DateRangeQuery.Absolute.class, name = "absolute"),
        @JsonSubTypes.Type(value = DateRangeQuery.Relative.class, name = "relative") })
public interface DateRangeQuery {
    @Data
    static class Absolute implements DateRangeQuery {
        private final long start;
        private final long end;

        @JsonCreator
        public static Absolute create(
                @JsonProperty(value = "start", required = true) long start,
                @JsonProperty(value = "end", required = true) long end) {
            return new Absolute(start, end);
        }

        @Override
        public DateRange buildDateRange() {
            return new DateRange(start, end);
        }
    }

    @Data
    static class Relative implements DateRangeQuery {
        public static final TimeUnit DEFAULT_UNIT = TimeUnit.DAYS;
        public static final long DEFAULT_VALUE = 1;

        private final TimeUnit unit;
        private final long value;

        @JsonCreator
        public static Relative create(@JsonProperty("unit") TimeUnit unit,
                @JsonProperty("value") Long value) {
            if (unit == null)
                unit = DEFAULT_UNIT;

            if (value == null)
                value = DEFAULT_VALUE;

            return new Relative(unit, value);
        }

        private long start(final Date now) {
            return now.getTime() - TimeUnit.MILLISECONDS.convert(value, unit);
        }

        private long end(final Date now) {
            return now.getTime();
        }

        @Override
        public DateRange buildDateRange() {
            final Date now = new Date();
            return new DateRange(start(now), end(now));
        }
    }

    DateRange buildDateRange();
}