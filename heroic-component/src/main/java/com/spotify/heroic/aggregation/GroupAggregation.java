package com.spotify.heroic.aggregation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@EqualsAndHashCode(of = { "NAME" }, callSuper = true)
public class GroupAggregation extends GroupingAggregation {
    public static final Map<String, String> ALL_GROUP = ImmutableMap.of();
    public static final String NAME = "group";

    @JsonCreator
    public GroupAggregation(@JsonProperty("of") List<String> of, @JsonProperty("each") Aggregation each) {
        super(of, each);
    }

    /**
     * Generate a key for this specific group.
     * 
     * @param tags The tags of a specific group.
     * @return
     */
    protected Map<String, String> key(final Map<String, String> tags) {
        final List<String> of = getOf();

        if (of == null)
            return tags;

        // group by 'everything'
        if (of.isEmpty())
            return ALL_GROUP;

        final Map<String, String> key = new HashMap<>();

        for (final String o : of)
            key.put(o, tags.get(o));

        return key;
    }

    @ToString
    @RequiredArgsConstructor
    private static class GroupTraverseState {
        private int count = 0;
        private final TraverseState state;
        private final Map<KeyValue, Integer> values = new HashMap<>();
    }

    @ToString
    @RequiredArgsConstructor
    @EqualsAndHashCode
    private static class KeyValue {
        private final String key;
        private final String value;
    }
}
