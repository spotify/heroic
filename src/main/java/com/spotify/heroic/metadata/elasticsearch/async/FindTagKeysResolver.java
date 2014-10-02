package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.HashSet;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchUtils;
import com.spotify.heroic.metadata.elasticsearch.model.FindTagKeys;

@RequiredArgsConstructor
public class FindTagKeysResolver implements Callback.Resolver<FindTagKeys> {
    private final Client client;
    private final String index;
    private final String type;
    private final FilterBuilder filter;

    @Override
    public FindTagKeys resolve() throws Exception {
        final SearchRequestBuilder request = client.prepareSearch(index).setTypes(type).setSearchType("count");

        request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter));

        {
            final AggregationBuilder<?> terms = AggregationBuilders.terms("terms").field(ElasticSearchUtils.TAGS_KEY)
                    .size(0);
            final AggregationBuilder<?> nested = AggregationBuilders.nested("nested").path(ElasticSearchUtils.TAGS)
                    .subAggregation(terms);
            request.addAggregation(nested);
        }

        final SearchResponse response = request.get();

        final Terms terms;

        {
            final Aggregations aggregations = response.getAggregations();
            final Nested attributes = (Nested) aggregations.get("nested");
            terms = (Terms) attributes.getAggregations().get("terms");
        }

        final Set<String> keys = new HashSet<String>();

        for (final Terms.Bucket bucket : terms.getBuckets()) {
            keys.add(bucket.getKey());
        }

        return new FindTagKeys(keys, keys.size());
    }
}
