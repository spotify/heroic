package com.spotify.heroic.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.test.FakeModuleLoader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static com.spotify.heroic.filter.Filter.and;
import static com.spotify.heroic.filter.Filter.hasTag;
import static com.spotify.heroic.filter.Filter.matchKey;
import static com.spotify.heroic.filter.Filter.matchTag;
import static com.spotify.heroic.filter.Filter.not;
import static com.spotify.heroic.filter.Filter.or;
import static com.spotify.heroic.filter.Filter.regex;
import static com.spotify.heroic.filter.Filter.startsWith;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class FilterSerializerTest {
    @Mock
    private QueryParser parser;

    private ObjectMapper m = FakeModuleLoader.builder().build().json();;

    @Test
    public void serializeTest() throws IOException {
        checkFilter(matchTag("a", "b"), "[\"=\",\"a\",\"b\"]");
        checkFilter(startsWith("a", "b"), "[\"^\",\"a\",\"b\"]");
        checkFilter(hasTag("a"), "[\"+\",\"a\"]");
        checkFilter(matchKey("a"), "[\"key\",\"a\"]");
        checkFilter(regex("a", "b"), "[\"~\",\"a\",\"b\"]");
        checkFilter(not(matchTag("a", "b")), "[\"not\",[\"=\",\"a\",\"b\"]]");

        checkFilter(and(matchTag("a", "b"), matchTag("c", "d")),
            "[\"and\",[\"=\",\"a\",\"b\"],[\"=\",\"c\",\"d\"]]");

        checkFilter(or(matchTag("a", "b"), matchTag("c", "d")),
            "[\"or\",[\"=\",\"a\",\"b\"],[\"=\",\"c\",\"d\"]]");
    }

    private void checkFilter(final Filter filter, final String json) throws IOException {
        final Filter f = filter.optimize();

        assertEquals(f, m.readValue(json, Filter.class));
        assertEquals(json, m.writeValueAsString(f));
    }
}
