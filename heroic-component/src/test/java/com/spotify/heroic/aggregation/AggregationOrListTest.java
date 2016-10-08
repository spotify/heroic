package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class AggregationOrListTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = new ObjectMapper();
        mapper.registerSubtypes(new NamedType(Empty.class, Empty.NAME));
        mapper.addMixIn(Aggregation.class, TypeNameMixin.class);
    }

    @Test
    public void deserialize() throws Exception {
        assertEquals(Optional.empty(), deserialize("[]"));
        assertEquals(Optional.of(Empty.INSTANCE), deserialize("[{\"type\":\"empty\"}]"));
        assertEquals(Optional.of(Empty.INSTANCE), deserialize("{\"type\":\"empty\"}"));
    }

    @Test
    public void fail() throws Exception {
        expected.expect(JsonMappingException.class);
        expected.expectMessage("Unexpected token");

        deserialize("42");
    }

    private Optional<Aggregation> deserialize(final String content) throws Exception {
        return mapper.readValue(content, AggregationOrList.class).toAggregation();
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    public interface TypeNameMixin {
    }
}
