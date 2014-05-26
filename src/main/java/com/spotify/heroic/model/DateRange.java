package com.spotify.heroic.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString(of = {"start", "end"})
@EqualsAndHashCode(of = {"start", "end"})
public class DateRange implements Comparable<DateRange> {
    @Getter
    private final long start;

    @Getter
    private final long end;

    public DateRange(long start, long end) {
        if (start > end)
            throw new IllegalArgumentException("'start' time must not be after 'end'");

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

    public DateRange roundToInterval(long interval) {
        final long newStart = start - (start % interval);
        final long newEnd = end - end % interval;
        return new DateRange(newStart, newEnd);
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

    public boolean contains(long t) {
        return t >= start && t <= end;
    }

    /**
     * Modify this range with another range.
     *
     * A modification asserts that the new range is a subset of the current range.
     * Any span which would cause the new range to become out of bounds will be cropped.
     *
     * @param range The constraints to modify this range against.
     * @return A new range representing the modified range.
     */
    public DateRange modify(DateRange range) {
        return new DateRange(Math.max(start, range.getStart()), Math.min(end, range.getEnd()));
    }
}
