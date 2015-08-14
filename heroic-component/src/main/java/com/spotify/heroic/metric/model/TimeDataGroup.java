package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.MetricType;
import com.spotify.heroic.model.TimeData;

@Data
public class TimeDataGroup {
    final MetricType type;
    final List<? extends TimeData> data;

    /**
     * Helper method to fetch a collection of the given type, if applicable.
     *
     * @param expected The expected type to read.
     * @return A list of the expected type.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getDataAs(Class<T> expected) {
        if (expected.isAssignableFrom(type.type())) {
            throw new IllegalArgumentException(
                    String.format("Cannot assign type (%s) to expected (%s)", type, expected));
        }

        return (List<T>) data;
    }
}