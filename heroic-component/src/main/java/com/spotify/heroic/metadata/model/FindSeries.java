package com.spotify.heroic.metadata.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.Reducer;
import com.spotify.heroic.model.Series;

@Data
public class FindSeries {
    public static final FindSeries EMPTY = new FindSeries(new HashSet<Series>(), 0, 0);

    private final Set<Series> series;
    private final int size;
    private final int duplicates;

    public static class SelfReducer implements Reducer<FindSeries, FindSeries> {
        @Override
        public FindSeries resolved(Collection<FindSeries> results, Collection<CancelReason> cancelled) throws Exception {
            final Set<Series> series = new HashSet<Series>();
            int size = 0;
            int duplicates = 0;

            for (final FindSeries result : results) {
                for (final Series s : result.getSeries()) {
                    if (series.add(s)) {
                        duplicates += 1;
                    }
                }

                duplicates += result.getDuplicates();
                size += result.getSize();
            }

            return new FindSeries(series, size, duplicates);
        }
    };

    private static final SelfReducer reducer = new SelfReducer();

    public static Reducer<FindSeries, FindSeries> reduce() {
        return reducer;
    }

    @JsonCreator
    public static FindSeries create(@JsonProperty("series") Set<Series> series, @JsonProperty("size") int size,
            @JsonProperty("duplicates") int duplicates) {
        return new FindSeries(series, size, duplicates);
    }
}