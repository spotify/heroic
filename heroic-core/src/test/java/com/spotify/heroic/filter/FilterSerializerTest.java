package com.spotify.heroic.filter;

import static com.spotify.heroic.filter.Filter.and;
import static com.spotify.heroic.filter.Filter.hasTag;
import static com.spotify.heroic.filter.Filter.matchKey;
import static com.spotify.heroic.filter.Filter.matchTag;
import static com.spotify.heroic.filter.Filter.not;
import static com.spotify.heroic.filter.Filter.or;
import static com.spotify.heroic.filter.Filter.regex;
import static com.spotify.heroic.filter.Filter.startsWith;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.test.FakeModuleLoader;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FilterSerializerTest {
    private ObjectMapper m = FakeModuleLoader.builder().build().json();

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

        checkFilter(TrueFilter.create(), "[\"true\"]");
        checkFilter(FalseFilter.create(), "[\"false\"]");
    }

    /**
     * Check that object type-based variants work as expected.
     */
    @Test
    public void objectTest() throws IOException {
        checkObjectFilter(matchTag("a", "b"),
            "{\"type\":\"matchTag\",\"tag\":\"a\",\"value\":\"b\"}");

        checkObjectFilter(startsWith("a", "b"),
            "{\"type\":\"startsWith\",\"tag\":\"a\",\"value\":\"b\"}");
        checkObjectFilter(hasTag("a"), "{\"type\":\"hasTag\",\"tag\":\"a\"}");
        checkObjectFilter(matchKey("a"), "{\"type\":\"key\",\"key\":\"a\"}");
        checkObjectFilter(regex("a", "b"), "{\"type\":\"regex\",\"tag\":\"a\",\"value\":\"b\"}");
        checkObjectFilter(not(matchTag("a", "b")),
            "{\"type\":\"not\",\"filter\":{\"type\":\"matchTag\",\"tag\":\"a\",\"value\":\"b\"}}");

        checkObjectFilter(and(matchTag("a", "b"), matchTag("c", "d")),
            "{\"type\":\"and\",\"filters\":[{\"type\":\"matchTag\",\"tag\":\"a\"," +
                "\"value\":\"b\"}," + "{\"type\":\"matchTag\",\"tag\":\"c\",\"value\":\"d\"}]}");

        checkObjectFilter(or(matchTag("a", "b"), matchTag("c", "d")),
            "{\"type\":\"or\",\"filters\":[{\"type\":\"matchTag\",\"tag\":\"a\",\"value\":\"b\"}," +
                "{\"type\":\"matchTag\",\"tag\":\"c\",\"value\":\"d\"}]}");
    }

    private void checkFilter(final Filter filter, final String json) throws IOException {
        final Filter f = filter.optimize();

        assertEquals(f, m.readValue(json, Filter.class));
        assertEquals(json, m.writeValueAsString(f));
    }

    private void checkObjectFilter(final Filter filter, final String json) throws IOException {
        final Filter f = filter.optimize();

        assertEquals(f, m.readValue(json, Filter.class));
    }
}
