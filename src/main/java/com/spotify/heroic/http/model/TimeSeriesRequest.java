package com.spotify.heroic.http.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.filter.AndFilter;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.model.filter.HasTagFilter;
import com.spotify.heroic.model.filter.MatchKeyFilter;
import com.spotify.heroic.model.filter.MatchTagFilter;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeSeriesRequest {
    /**
     * Only include time series which match the exact key.
     */
    private final String matchKey;

    /**
     * Only include time series which matches the exact key/value combination.
     */
    private final Map<String, String> matchTags;

    /**
     * Only include time series which has the following tags.
     */
    private final Set<String> hasTags;

    private final Filter filter;

    @JsonCreator
    public static TimeSeriesRequest create(
            @JsonProperty("matchKey") String matchKey,
            @JsonProperty("matchTags") Map<String, String> matchTags,
            @JsonProperty("hasTags") Set<String> hasTags,
            @JsonProperty("filter") Filter filter) {
        return new TimeSeriesRequest(matchKey, matchTags, hasTags, filter);
    }

    public Filter makeFilter() {
        final List<Filter> statements = new ArrayList<>();

        if (filter != null) {
            statements.add(filter);
        }

        if (matchTags != null && !matchTags.isEmpty()) {
            for (final Map.Entry<String, String> entry : matchTags.entrySet()) {
                statements.add(new MatchTagFilter(entry.getKey(), entry
                        .getValue()));
            }
        }

        if (hasTags != null && !hasTags.isEmpty()) {
            for (final String tag : hasTags) {
                statements.add(new HasTagFilter(tag));
            }
        }

        if (matchKey != null)
            statements.add(new MatchKeyFilter(matchKey));

        if (statements.size() == 0) {
            return null;
        }

        if (statements.size() == 1)
            return statements.get(0).optimize();

        return new AndFilter(statements).optimize();
    }
}
