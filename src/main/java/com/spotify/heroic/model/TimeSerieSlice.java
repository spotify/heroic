package com.spotify.heroic.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString(of = { "timeSerie", "start", "end" })
@EqualsAndHashCode(of = { "timeSerie", "start", "end" })
public class TimeSerieSlice {
    @Getter
    private final TimeSerie timeSerie;
    @Getter
    private final long start;
    @Getter
    private final long end;

    public TimeSerieSlice(TimeSerie timeSerie, long start, long end) {
        this.timeSerie = timeSerie;
        this.start = start;
        this.end = end;
    }

    public TimeSerieSlice join(TimeSerieSlice other) {
        if (!overlap(other)) {
            throw new IllegalArgumentException("Slices " + this + " and "
                    + other + " does not overlap");
        }

        long start = Math.min(this.start, other.start);
        long end = Math.max(this.end, other.end);

        return new TimeSerieSlice(timeSerie, start, end);
    }

    public boolean overlap(TimeSerieSlice other) {
        if (!timeSerie.equals(other.timeSerie)) {
            return false;
        }

        if (end < other.start) {
            return false;
        }

        if (start > other.end) {
            return false;
        }

        return true;
    }

    private static final Comparator<TimeSerieSlice> JOIN_ALL_COMPARATOR = new Comparator<TimeSerieSlice>() {
        @Override
        public int compare(TimeSerieSlice o1, TimeSerieSlice o2) {
            return Long.compare(o1.start, o2.start);
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

        long end = first.getEnd();

        while (iterator.hasNext()) {
            final TimeSerieSlice tail = iterator.next();

            if (!first.getTimeSerie().equals(tail.getTimeSerie())) {
                throw new IllegalArgumentException("Cannot join slices "
                        + first + " and " + tail
                        + " from different time series");
            }

            if (tail.getStart() > first.getEnd()) {
                joined.add(new TimeSerieSlice(first.getTimeSerie(), first
                        .getStart(), end));
                return tail;
            }

            end = Math.max(end, tail.getEnd());
        }

        joined.add(new TimeSerieSlice(first.getTimeSerie(), first.getStart(),
                end));

        return null;
    }
}
