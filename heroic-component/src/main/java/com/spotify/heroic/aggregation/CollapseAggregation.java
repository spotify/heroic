package com.spotify.heroic.aggregation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@EqualsAndHashCode(of = { "NAME" }, callSuper = true)
public class CollapseAggregation extends GroupingAggregation {
    public static final Map<String, String> NO_GROUP = ImmutableMap.of();
    public static final String NAME = "collapse";

    @JsonCreator
    public CollapseAggregation(@JsonProperty("of") List<String> of, @JsonProperty("each") Aggregation each) {
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
            return NO_GROUP;

        // group by 'everything'
        if (of.isEmpty())
            return tags;

        final Map<String, String> key = new HashMap<>(tags);

        for (final String o : of)
            key.remove(o);

        return key;
    }
}