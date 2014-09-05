package com.spotify.heroic.filter.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.ManyTermsFilter;

public abstract class ManyTermsSerialization<T extends ManyTermsFilter> implements
        FilterSerialization<T> {
    @Override
    public T deserialize(JsonParser p, DeserializationContext c)
            throws IOException, JsonProcessingException {
        final List<Filter> statements = new ArrayList<>();

        while (p.nextToken() != JsonToken.END_ARRAY)
            statements.add(p.readValueAs(Filter.class));

        return build(statements);
    }

    @Override
    public void serialize(JsonGenerator g, ManyTermsFilter f)
            throws IOException {
        for (final Filter filter : f.terms())
            g.writeObject(filter);
    }

    protected abstract T build(List<Filter> terms);
}