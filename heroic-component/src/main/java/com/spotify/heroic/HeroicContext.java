package com.spotify.heroic;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationSerializer.Serializer;
import com.spotify.heroic.aggregation.model.AggregationQuery;

public interface HeroicContext {
    <T extends Aggregation, R extends AggregationQuery> void aggregation(short id, Class<T> type, Class<R> queryType,
            Serializer<T> serializer);
}
