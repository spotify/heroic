package com.spotify.heroic.model;

import java.sql.Date;

import lombok.Data;

import org.apache.commons.lang.time.FastDateFormat;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Data
public class DateRange implements Comparable<DateRange> {
    private final long start;
    private final long end;

    public DateRange(long start, long end) {
        if (start > end)
            start = end;

        this.start = start;
        this.end = end;
    }

    public long start() {
        return start;
    }

    public long end() {
        return end;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return diff() == 0;
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
     * A modification asserts that the new range is a subset of the current
     * range. Any span which would cause the new range to become out of bounds
     * will be cropped.
     * 
     * @param range
     *            The constraints to modify this range against.
     * @return A new range representing the modified range.
     */
    public DateRange modify(DateRange range) {
        return modify(range.getStart(), range.getEnd());
    }

    public DateRange modify(long start, long end) {
        return new DateRange(Math.max(start, start), Math.min(end, end));
    }

    public DateRange withStart(long start) {
        return new DateRange(start, this.end);
    }

    public DateRange withEnd(long end) {
        return new DateRange(this.start, end);
    }

    private static final FastDateFormat format = FastDateFormat
            .getInstance("yyyy-MM-dd HH:mm");

    @Override
    public String toString() {
        final Date start = new Date(this.start);
        final Date end = new Date(this.end);
        return "DateRange(start=" + format.format(start) + ", end="
                + format.format(end) + ")";
    }
}
