package com.spotify.heroic.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;

public class GrokProcessor {
    private final Grok instance;

    // @formatter:off
    private static final Map<String, String> DEFAULT_PATTERNS = ImmutableMap.of(
        "site", "[a-z]+",
        "pod", "[a-z]+[0-9]+",
        "pool", "[a-z]+[0-9]+",
        "role", "[a-z][a-z0-9]+",
        "domain", "[a-z0-9-]+\\.[a-z]+"
    );
    // @formatter:on

    @JsonCreator
    public GrokProcessor(@JsonProperty("patterns") Map<String, String> patterns,
            @JsonProperty("pattern") String pattern) {
        checkNotNull(patterns, "patterns");
        checkNotNull(pattern, "pattern");

        final Grok grok = new Grok();

        try {
            for (final Map.Entry<String, String> e : DEFAULT_PATTERNS.entrySet()) {
                grok.addPattern(e.getKey(), e.getValue());
            }

            for (final Map.Entry<String, String> e : patterns.entrySet()) {
                grok.addPattern(e.getKey(), e.getValue());
            }

            grok.compile(pattern);
        } catch(final Exception e) {
            throw new RuntimeException(e);
        }

        this.instance = grok;
    }

    public Map<String, Object> parse(final String input) {
        final Match m = instance.match(input);

        if (m == Match.EMPTY) {
            return ImmutableMap.of();
        }

        m.captures();
        return m.toMap();
    }
}