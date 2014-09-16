package com.spotify.heroic.http.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.TrueFilter;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataQueryBody {
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

    /**
     * A general set of filters. If this is combined with the other mechanisms,
     * all the filters will be AND:ed together.
     */
    private final Filter filter;

    @JsonCreator
    public static MetadataQueryBody create(
            @JsonProperty("matchKey") String matchKey,
            @JsonProperty("matchTags") Map<String, String> matchTags,
            @JsonProperty("hasTags") Set<String> hasTags,
            @JsonProperty("filter") Filter filter) {
        return new MetadataQueryBody(matchKey, matchTags, hasTags, filter);
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

        if (statements.size() == 0)
            return TrueFilter.get();

        if (statements.size() == 1)
            return statements.get(0).optimize();

        return new AndFilter(statements).optimize();
    }
}
