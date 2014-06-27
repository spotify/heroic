package com.spotify.heroic.metrics.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
@ToString(of = { "key", "range", "filter", "groupBy" })
public class FindTimeSeriesCriteria {
    @Getter
    private final String key;
    @Getter
    private final Map<String, String> filter;
    @Getter
    private final Map<String, String> group;
    @Getter
    private final List<String> groupBy;
    @Getter
    private final DateRange range;

    public FindTimeSeriesCriteria withRange(DateRange range) {
        return new FindTimeSeriesCriteria(key, filter, group, groupBy, range);
    }

    public FindTimeSeriesCriteria withFilter(Map<String, String> filter) {
        return new FindTimeSeriesCriteria(key, filter, group, groupBy, range);
    }

    /**
     * Build a new filter that includes criteria from the specified set of
     * tags if they are not already present.
     *
     * @param tags
     */
    public FindTimeSeriesCriteria overlapFilter(Map<String, String> tags) {
        final Map<String, String> filter = new HashMap<String, String>(this.filter);

        for (final Map.Entry<String, String> entry : tags.entrySet()) {
            if (filter.get(entry.getKey()) != null)
                continue;

            filter.put(entry.getKey(), entry.getValue());
        }

        return withFilter(filter);
    }
}