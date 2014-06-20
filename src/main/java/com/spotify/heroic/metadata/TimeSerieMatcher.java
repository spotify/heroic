package com.spotify.heroic.metadata;

import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
class TimeSerieMatcher {
    private final TimeSerieQuery query;

    public boolean matches(TimeSerie timeserie) {
        if (query.getMatchKey() != null
                && !query.getMatchKey().equals(timeserie.getKey())) {
            return false;
        }

        if (!matchingTags(timeserie.getTags(), query.getMatchTags(),
                query.getHasTags())) {
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
}