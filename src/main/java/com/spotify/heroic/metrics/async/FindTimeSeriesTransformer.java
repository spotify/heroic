package com.spotify.heroic.metrics.async;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.model.Series;

/**
 * Transforms a metadata time series result with a metrics time serie result.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class FindTimeSeriesTransformer implements
Callback.Transformer<FindSeries, FindTimeSeriesGroups> {
    private final List<String> groupBy;

    @Override
    public FindTimeSeriesGroups transform(final FindSeries result)
            throws Exception {
        final Map<Series, Set<Series>> groups = new HashMap<Series, Set<Series>>();

        for (final Series series : result.getSeries()) {
            final Map<String, String> tags = new HashMap<>();

            if (groupBy != null) {
                for (final String group : groupBy) {
                    tags.put(group, series.getTags().get(group));
                }
            }

            final Series key = series.withTags(tags);

            Set<Series> group = groups.get(key);

            if (group == null) {
                group = new HashSet<>();
                groups.put(key, group);
            }

            group.add(series);
        }

        return new FindTimeSeriesGroups(groups);
    }
};
