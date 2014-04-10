package com.spotify.heroic.backend;

import java.util.Map;
import java.util.Set;

public class FilteringTimeSerieMatcher implements TimeSerieMatcher {
    private final String matchKey;
    private final Map<String, String> matchTags;
    private final Set<String> hasTags;

    public FilteringTimeSerieMatcher(String matchKey,
            Map<String, String> matchTags, Set<String> hasTags) {
        this.matchKey = matchKey;
        this.matchTags = matchTags;
        this.hasTags = hasTags;
    }

    @Override
    public boolean matches(TimeSerie timeserie) {
        if (matchKey != null && !matchKey.equals(timeserie.getKey())) {
            return false;
        }

        if (!matchingTags(timeserie.getTags(), matchTags, hasTags)) {
            return false;
        }

        return true;
    }

    private boolean matchingTags(Map<String, String> tags,
            Map<String, String> matchTags, Set<String> hasTags) {
        // query not specified.
        if (matchTags != null) {
            // match the row tags with the query tags.
            for (final Map.Entry<String, String> entry : matchTags.entrySet()) {
                // check tags for the actual row.
                final String tagValue = tags.get(entry.getKey());

                if (tagValue == null || entry.getValue() == null
                        || !tagValue.equals(entry.getValue())) {
                    return false;
                }
            }
        }

        if (hasTags != null) {
            for (final String tag : hasTags) {
                if (!tags.containsKey(tag)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public String indexKey() {
        return matchKey;
    }

    @Override
    public Map<String, String> indexTags() {
        return matchTags;
    }
}