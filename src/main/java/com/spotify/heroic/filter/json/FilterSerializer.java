package com.spotify.heroic.filter.json;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.TrueFilter;

public class FilterSerializer extends JsonSerializer<Filter> {
    private static final Map<Class<? extends Filter>, FilterSerialization<? extends Filter>> IMPL = new HashMap<>();

    static {
        IMPL.put(MatchTagFilter.class, JsonCommon.MATCH_TAG);
        IMPL.put(StartsWithFilter.class, JsonCommon.STARTS_WITH);
        IMPL.put(RegexFilter.class, JsonCommon.REGEX);
        IMPL.put(HasTagFilter.class, JsonCommon.HAS_TAG);
        IMPL.put(MatchKeyFilter.class, JsonCommon.MATCH_KEY);
        IMPL.put(AndFilter.class, JsonCommon.AND);
        IMPL.put(OrFilter.class, JsonCommon.OR);
        IMPL.put(NotFilter.class, JsonCommon.NOT);
        IMPL.put(TrueFilter.class, JsonCommon.TRUE);
        IMPL.put(FalseFilter.class, JsonCommon.FALSE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void serialize(Filter value, JsonGenerator g, SerializerProvider provider) throws IOException,
            JsonProcessingException {

        final FilterSerialization<Filter> serializer = (FilterSerialization<Filter>) IMPL.get(value.getClass());

        if (serializer == null)
            throw new JsonGenerationException("Filter type not supported: " + value.getClass());

        g.writeStartArray();
        g.writeString(value.operator());
        serializer.serialize(g, value);
        g.writeEndArray();
    }
}