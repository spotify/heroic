package com.spotify.heroic;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.Empty;
import com.spotify.heroic.aggregation.Group;
import com.spotify.heroic.common.TypeNameMixin;

import java.io.InputStream;

import org.junit.Before;
import org.junit.Test;

public class QueryTest {
    public InputStream resource(final String name) {
        return getClass().getClassLoader().getResourceAsStream(
                getClass().getPackage().getName().replace('.', '/') + '/' + name);
    }

    private ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = new ObjectMapper();
        mapper.addMixIn(Aggregation.class, TypeNameMixin.class);
        mapper.registerModule(new Jdk8Module());
        mapper.registerSubtypes(new NamedType(Group.class, Group.NAME));
        mapper.registerSubtypes(new NamedType(Empty.class, Empty.NAME));
    }

    private Query parse(final String name) throws Exception {
        try (final InputStream in = resource(name)) {
            return mapper.readValue(in, Query.class);
        }
    }

    @Test
    public void testAggregationCompatibility() throws Exception {
        final Query a = parse("Query.AggregationCompat.1.json");
        final Query b = parse("Query.AggregationCompat.2.json");
        final Query c = parse("Query.AggregationCompat.3.json");

        assertEquals(a, b);
        assertEquals(a, c);
    }
}
