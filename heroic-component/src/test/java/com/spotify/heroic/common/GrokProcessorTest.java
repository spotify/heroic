package com.spotify.heroic.common;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class GrokProcessorTest {
    @Test
    public void testHost() throws Exception {
        final GrokProcessor grok =
            new GrokProcessor(ImmutableMap.of(), "%{pod}-%{role}-%{pool}\\.%{site}\\.%{domain}");
        Assert.assertEquals(
            ImmutableMap.of("pod", "sto1", "role", "example", "pool", "a1", "site", "sto", "domain",
                "example.com"), grok.parse("sto1-example-a1.sto.example.com"));
    }
}
