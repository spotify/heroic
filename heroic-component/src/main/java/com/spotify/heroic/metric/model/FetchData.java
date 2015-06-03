package com.spotify.heroic.metric.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;

import eu.toolchain.async.Collector;

@Data
public class FetchData<T extends TimeData> {
    private final Series series;
    private final List<T> data;
    private final List<Long> times;

    public static <T extends TimeData> Collector<FetchData<T>, FetchData<T>> merger(final Series series) {
        return new Collector<FetchData<T>, FetchData<T>>() {
            @Override
            public FetchData<T> collect(Collection<FetchData<T>> results) throws Exception {
                final List<T> data = new ArrayList<>();
                final List<Long> times = new ArrayList<Long>();

                for (final FetchData<T> fetch : results) {
                    data.addAll(fetch.getData());
                    times.addAll(fetch.getTimes());
                }

                Collections.sort(data);
                return new FetchData<T>(series, data, times);
            }
        };
    }
}