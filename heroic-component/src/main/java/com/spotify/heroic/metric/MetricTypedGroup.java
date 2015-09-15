package com.spotify.heroic.metric;

import java.util.List;

import lombok.Data;

@Data
public class MetricTypedGroup {
    final MetricType type;
    final List<? extends Metric> data;

    /**
     * Helper method to fetch a collection of the given type, if applicable.
     *
     * @param expected The expected type to read.
     * @return A list of the expected type.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getDataAs(Class<T> expected) {
        if (!expected.isAssignableFrom(type.type())) {
            throw new IllegalArgumentException(
                    String.format("Cannot assign type (%s) to expected (%s)", type, expected));
        }

        return (List<T>) data;
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }
}