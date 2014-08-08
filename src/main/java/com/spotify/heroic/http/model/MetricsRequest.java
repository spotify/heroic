package com.spotify.heroic.http.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.aggregation.Aggregation;

@ToString(of = { "key", "tags", "groupBy", "range", "noCache", "aggregators" })
@EqualsAndHashCode(of = { "key", "tags", "groupBy", "range", "noCache",
"aggregators" })
public class MetricsRequest {
    private static final DateRangeRequest DEFAULT_DATE_RANGE = new RelativeDateRangeRequest(
            TimeUnit.DAYS, 7);
    private static final List<AggregationRequest> EMPTY_AGGREGATIONS = new ArrayList<>();

    @Getter
    private final String key = null;

    @Getter
    private final Map<String, String> tags = new HashMap<String, String>();

    @Getter
    private final List<String> groupBy = new ArrayList<String>();

    @Getter
    private final DateRangeRequest range = DEFAULT_DATE_RANGE;

    @Getter
    private final boolean noCache = false;

    @Getter
    private final List<AggregationRequest> aggregators = EMPTY_AGGREGATIONS;

    public List<Aggregation> makeAggregators() {
        final List<Aggregation> aggregators = new ArrayList<>(
                this.aggregators.size());

        for (final AggregationRequest aggregation : this.aggregators) {
            aggregators.add(aggregation.makeAggregation());
        }

        return aggregators;
    }
}
