package com.spotify.heroic.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString(of = { "timeSerie", "range" })
@EqualsAndHashCode(of = { "timeSerie", "range" })
public class TimeSerieSlice {
    @Getter
    private final TimeSerie timeSerie;
    @Getter
    private final DateRange range;

    public TimeSerieSlice(TimeSerie timeSerie, DateRange range) {
        this.timeSerie = timeSerie;
        this.range = range;
    }

    public TimeSerieSlice join(TimeSerieSlice other) {
        if (!overlap(other)) {
            throw new IllegalArgumentException("Slices " + this + " and "
                    + other + " does not overlap");
        }

        return new TimeSerieSlice(timeSerie, range.join(other.range));
    }

    public boolean overlap(TimeSerieSlice other) {
        if (!timeSerie.equals(other.timeSerie)) {
            return false;
        }

        /* cannot be determined. */
        if (range == null || other.range == null) {
            return false;
        }

        if (range.overlap(other.range)) {
            return false;
        }

        return true;
    }

    /**
     * Create an expanded time slice.
     * 
     * This will cause the current time slice to be expanded if it the specified
     * range is larger.
     * 
     * @param first
     * @param last
     * @return
     */
    public TimeSerieSlice modify(long start, long end) {
        return new TimeSerieSlice(timeSerie, new DateRange(start, end));
    }

    private static final Comparator<TimeSerieSlice> JOIN_ALL_COMPARATOR = new Comparator<TimeSerieSlice>() {
        @Override
        public int compare(TimeSerieSlice o1, TimeSerieSlice o2) {
            return o1.range.compareTo(o2.range);
        }
    };

    /**
     * Join a list of slices into another list. This will make all overlapping
     * slices into one.
     * 
     * @param slices
     *            List of slices to join.
     * @return A new list of non-overlapping slices.
     */
    public static List<TimeSerieSlice> joinAll(final List<TimeSerieSlice> slices) {
        final List<TimeSerieSlice> sorted = new ArrayList<TimeSerieSlice>(
                slices);
        Collections.sort(sorted, JOIN_ALL_COMPARATOR);

        final List<TimeSerieSlice> joined = new ArrayList<TimeSerieSlice>();
        final Iterator<TimeSerieSlice> iterator = sorted.iterator();

        if (!iterator.hasNext())
            return joined;

        TimeSerieSlice next = iterator.next();

        while (next != null) {
            next = joinAllIterate(joined, next, iterator);
        }

        return joined;
    }

    private static TimeSerieSlice joinAllIterate(
            final List<TimeSerieSlice> joined, final TimeSerieSlice first,
            Iterator<TimeSerieSlice> iterator) {

        long end = first.range.end();

        while (iterator.hasNext()) {
            final TimeSerieSlice tail = iterator.next();

            if (!first.getTimeSerie().equals(tail.getTimeSerie())) {
                throw new IllegalArgumentException("Cannot join slices "
                        + first + " and " + tail
                        + " from different time series");
            }

            if (tail.range.start() > end) {
                final DateRange range = new DateRange(first
                        .range.start(), end);
                joined.add(new TimeSerieSlice(first.getTimeSerie(), range));
                return tail;
            }

            end = Math.max(end, tail.range.end());
        }

        final DateRange range = new DateRange(first.range.start(), end);
        joined.add(new TimeSerieSlice(first.getTimeSerie(), range));
        return null;
    }

    public long getWidth() {
        return range.diff();
    }
}
