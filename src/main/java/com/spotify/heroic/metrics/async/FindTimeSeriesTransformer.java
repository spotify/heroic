package com.spotify.heroic.metrics.async;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.model.TimeSerie;

/**
 * Transforms a metadata time series result with a metrics time serie result.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class FindTimeSeriesTransformer implements Callback.Transformer<FindTimeSeries, FindTimeSeriesGroups> {
    private final Map<String, String> groupKey;
    private final List<String> groupBy;

    @Override
    public FindTimeSeriesGroups transform(final FindTimeSeries result) throws Exception {
        final Map<TimeSerie, Set<TimeSerie>> groups = new HashMap<TimeSerie, Set<TimeSerie>>();

        for (final TimeSerie timeSerie : result.getTimeSeries()) {
            final Map<String, String> tags = new HashMap<>(groupKey);

            if (groupBy != null) {
                for (final String group : groupBy) {
                    tags.put(group, timeSerie.getTags().get(group));
                }
            }

            final TimeSerie key = timeSerie.withTags(tags);

            Set<TimeSerie> group = groups.get(key);

            if (group == null) {
                group = new HashSet<>();
                groups.put(key, group);
            }

            group.add(timeSerie);
        }

        return new FindTimeSeriesGroups(groups);
    }
};
