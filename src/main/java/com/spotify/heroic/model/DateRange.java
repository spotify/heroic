package com.spotify.heroic.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(of = {"start", "end"})
@EqualsAndHashCode(of = {"start", "end"})
public class DateRange implements Comparable<DateRange> {
    @Getter
    @Setter
    private long start;

    @Getter
    @Setter
    private long end;

    public DateRange() {
    }

    public DateRange(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    public long diff() {
        return end - start;
    }

    public DateRange roundToInterval(long hint) {
        return new DateRange(start - (start % hint), end
                + (hint - (end % hint)));
    }

    public boolean overlap(DateRange other) {
        if (end < other.start) {
            return false;
        }

        if (start > other.end) {
            return false;
        }

        return true;
    }

    @Override
    public int compareTo(DateRange other) {
        return Long.compare(start, other.start);
    }

    public DateRange join(DateRange other) {
        long start = Math.min(this.start, other.start);
        long end = Math.max(this.end, other.end);
        return new DateRange(start, end);
    }
}
