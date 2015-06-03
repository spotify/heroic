package com.spotify.heroic.metric.generated.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.generated.Generator;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Event;
import com.spotify.heroic.model.Series;

public class SineGenerator implements Generator {
    private static final Map<String, Object> PAYLOAD = ImmutableMap.<String, Object> of();

    @Inject
    @Named("magnitude")
    private double magnitude;

    /**
     * How many milliseconds should be a full period (2 * PI).
     */
    @Inject
    @Named("period")
    private long period;

    /**
     * Frequency of data points in hertz.
     */
    @Inject
    @Named("step")
    private long step;

    @Override
    public List<DataPoint> generate(Series series, DateRange range, FetchQuotaWatcher watcher) {
        // calculate a consistent drift depending on which series is being fetched.
        double drift = Math.abs((double) series.hashCode() / (double) Integer.MAX_VALUE);

        final List<DataPoint> data = new ArrayList<>();

        final DateRange rounded = range.rounded(1000);

        if (!watcher.readData(range.diff() / step))
            throw new IllegalArgumentException("data limit reached");

        for (long time = rounded.getStart(); time < rounded.getEnd(); time += step) {
            double offset = ((double) (time % period)) / (double) period;
            double value = Math.sin(Math.PI * 2 * (offset + drift)) * magnitude;
            data.add(new DataPoint(time, value));
        }

        return data;
    }

    @Override
    public List<Event> generateEvents(Series series, DateRange range, FetchQuotaWatcher watcher) {
        final List<Event> data = new ArrayList<>();

        final DateRange rounded = range.rounded(1000);

        if (!watcher.readData(range.diff() / step))
            throw new IllegalArgumentException("data limit reached");

        for (long time = rounded.getStart(); time < rounded.getEnd(); time += step) {
            data.add(new Event(time, PAYLOAD));
        }

        return data;
    }
}
