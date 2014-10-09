package com.spotify.heroic.http.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.filter.Filter;

@Data
public class QueryMetrics {
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);
    private static final List<QueryAggregation> EMPTY_AGGREGATIONS = new ArrayList<>();

    private final String key = null;
    private final Map<String, String> tags = new HashMap<String, String>();
    private final Filter filter = null;
    private final List<String> groupBy = new ArrayList<String>();
    private final QueryDateRange range = DEFAULT_DATE_RANGE;
    private final boolean noCache = false;
    private final List<QueryAggregation> aggregators = EMPTY_AGGREGATIONS;

    public List<Aggregation> makeAggregators() {
        if (this.aggregators == null)
            return null;

        final List<Aggregation> aggregators = new ArrayList<>(this.aggregators.size());

        for (final QueryAggregation aggregation : this.aggregators) {
            aggregators.add(aggregation.makeAggregation());
        }

        return aggregators;
    }
}
