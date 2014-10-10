package com.spotify.heroic;

import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.model.QueryAggregation;

public class HeroicContextImpl implements HeroicContext {
    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private AggregationSerializer aggregationSerializer;

    @Override
    public <T extends Aggregation, R extends QueryAggregation> void registerAggregation(String name, Class<T> type,
            Class<R> queryType, short id, AggregationSerializer.Serializer<T> serializer) {
        mapper.registerSubtypes(new NamedType(type, name));
        mapper.registerSubtypes(new NamedType(queryType, name));
        aggregationSerializer.register(type, id, serializer);
    }
}
