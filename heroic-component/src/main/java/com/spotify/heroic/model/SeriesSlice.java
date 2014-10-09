package com.spotify.heroic.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString(of = { "series", "range" })
@EqualsAndHashCode(of = { "series", "range" })
public class SeriesSlice {
    @Getter
    private final Series series;
    @Getter
    private final DateRange range;

    public SeriesSlice(Series series, DateRange range) {
        this.series = series;
        this.range = range;
    }

    public SeriesSlice join(SeriesSlice other) {
        if (!overlap(other)) {
            throw new IllegalArgumentException("Slices " + this + " and " + other + " does not overlap");
        }

        return new SeriesSlice(series, range.join(other.range));
    }

    public boolean overlap(SeriesSlice other) {
        if (!series.equals(other.series)) {
            return false;
        }

        /* cannot be determined. */
        if (range == null || other.range == null) {
            return false;
        }

        if (!range.overlap(other.range)) {
            return false;
        }

        return true;
    }

    /**
     * Create an expanded time slice.
     *
     * This will cause the current time slice to be expanded if it the specified range is larger.
     *
     * @param first
     * @param last
     * @return
     */
    public SeriesSlice modify(long start, long end) {
        return modify(new DateRange(start, end));
    }

    public SeriesSlice modify(DateRange range) {
        return new SeriesSlice(series, this.range.modify(range));
    }

    private static final Comparator<SeriesSlice> JOIN_ALL_COMPARATOR = new Comparator<SeriesSlice>() {
        @Override
        public int compare(SeriesSlice o1, SeriesSlice o2) {
            return o1.range.compareTo(o2.range);
        }
    };

    /**
     * Join a list of slices into another list. This will make all overlapping slices into one.
     *
     * @param slices
     *            List of slices to join.
     * @return A new list of non-overlapping slices.
     */
    public static List<SeriesSlice> joinAll(final List<SeriesSlice> slices) {
        final List<SeriesSlice> sorted = new ArrayList<SeriesSlice>(slices);
        Collections.sort(sorted, JOIN_ALL_COMPARATOR);

        final List<SeriesSlice> joined = new ArrayList<SeriesSlice>();
        final Iterator<SeriesSlice> iterator = sorted.iterator();

        if (!iterator.hasNext())
            return joined;

        SeriesSlice next = iterator.next();

        while (next != null) {
            next = joinAllIterate(joined, next, iterator);
        }

        return joined;
    }

    private static SeriesSlice joinAllIterate(final List<SeriesSlice> joined, final SeriesSlice first,
            Iterator<SeriesSlice> iterator) {

        long end = first.range.end();

        while (iterator.hasNext()) {
            final SeriesSlice tail = iterator.next();

            if (!first.getSeries().equals(tail.getSeries())) {
                throw new IllegalArgumentException("Cannot join slices " + first + " and " + tail
                        + " from different time series");
            }

            if (tail.range.start() > end) {
                final DateRange range = new DateRange(first.range.start(), end);
                joined.add(new SeriesSlice(first.getSeries(), range));
                return tail;
            }

            end = Math.max(end, tail.range.end());
        }

        final DateRange range = new DateRange(first.range.start(), end);
        joined.add(new SeriesSlice(first.getSeries(), range));
        return null;
    }

    public long getWidth() {
        return range.diff();
    }

    public boolean contains(long f, long t) {
        return f <= t && range.contains(f) && range.contains(t);
    }

    public SeriesSlice modifyTags(Map<String, String> tags) {
        return new SeriesSlice(series.modifyTags(tags), range);
    }
}
