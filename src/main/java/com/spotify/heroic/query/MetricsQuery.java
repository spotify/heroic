package com.spotify.heroic.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@ToString(of = { "key", "tags", "range", "aggregators" })
public class MetricsQuery {
    private static final DateRange DEFAULT_DATE_RANGE = new RelativeDateRange(
            TimeUnit.DAYS, 7);

    @Getter
    @Setter
    private String key;

    @Getter
    @Setter
    private Map<String, String> tags;

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = AbsoluteDateRange.class, name = "absolute"),
            @JsonSubTypes.Type(value = RelativeDateRange.class, name = "relative") })
    private DateRange range = DEFAULT_DATE_RANGE;

    @Getter
    @Setter
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SumAggregator.class, name = "sum"),
            @JsonSubTypes.Type(value = AverageAggregator.class, name = "average") })
    private List<Aggregator> aggregators;

    public long getIntervalHint() {
        if (aggregators == null) {
            return 0;
        }

        long max = 0;

        for (final Aggregator aggregator : aggregators) {
            long hint = aggregator.getIntervalHint();

            if (hint > max) {
                max = hint;
            }
        }

        return max;
    }
}
