package com.spotify.heroic.metadata.elasticsearch;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import com.spotify.heroic.model.filter.AndFilter;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.model.filter.HasTagFilter;
import com.spotify.heroic.model.filter.MatchKeyFilter;
import com.spotify.heroic.model.filter.MatchTagFilter;
import com.spotify.heroic.model.filter.StartsWithFilter;
import com.spotify.heroic.model.filter.OrFilter;

public final class FilterUtils {
    public static final String TAGS_VALUE = "tags.value";
    public static final String TAGS_KEY = "tags.key";
    public static final String TAGS = "tags";
    public static final String KEY = "key";

    public static FilterBuilder convertFilter(final Filter filter) {
        if (filter == null)
            return null;

        if (filter instanceof AndFilter) {
            final AndFilter and = (AndFilter) filter;
            final List<FilterBuilder> filters = new ArrayList<>(and
                    .getStatements().size());

            for (final Filter stmt : and.getStatements())
                filters.add(convertFilter(stmt));

            return FilterBuilders.andFilter(filters
                    .toArray(new FilterBuilder[0]));
        }

        if (filter instanceof OrFilter) {
            final OrFilter or = (OrFilter) filter;
            final List<FilterBuilder> filters = new ArrayList<>(or
                    .getStatements().size());

            for (final Filter stmt : or.getStatements())
                filters.add(convertFilter(stmt));

            return FilterBuilders.orFilter(filters
                    .toArray(new FilterBuilder[0]));
        }

        if (filter instanceof MatchTagFilter) {
            final MatchTagFilter matchTag = (MatchTagFilter) filter;

            return FilterBuilders.nestedFilter(
                    TAGS,
                    FilterBuilders
                    .boolFilter()
                    .must(FilterBuilders.termFilter(TAGS_KEY,
                            matchTag.getTag()))
                            .must(FilterBuilders.termFilter(TAGS_VALUE,
                                    matchTag.getValue())));
        }

        if (filter instanceof StartsWithFilter) {
            final StartsWithFilter startsWith = (StartsWithFilter) filter;

            return FilterBuilders.nestedFilter(
                    TAGS,
                    FilterBuilders
                    .boolFilter()
                    .must(FilterBuilders.termFilter(TAGS_KEY,
                            startsWith.getTag()))
                            .must(FilterBuilders.prefixFilter(TAGS_VALUE,
                                    startsWith.getValue())));
        }

        if (filter instanceof HasTagFilter) {
            final HasTagFilter hasTag = (HasTagFilter) filter;
            return FilterBuilders.nestedFilter(TAGS,
                    FilterBuilders.termFilter(TAGS_KEY, hasTag.getTag()));
        }

        if (filter instanceof MatchKeyFilter) {
            final MatchKeyFilter matchKey = (MatchKeyFilter) filter;
            return FilterBuilders.termFilter(KEY, matchKey.getValue());
        }

        throw new IllegalArgumentException("Invalid filter statement: "
                + filter);
    }
}
