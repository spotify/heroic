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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metadata.model.FindKeys;

@RequiredArgsConstructor
public class FindKeysResolver implements Callback.Resolver<FindKeys> {
    private final Client client;
    private final String index;
    private final String type;
    private final FilterBuilder filter;

    @Override
    public FindKeys resolve() throws Exception {
        final SearchRequestBuilder request = client.prepareSearch(index)
                .setTypes(type).setSearchType("count");

        if (filter != null)
            request.setQuery(QueryBuilders.filteredQuery(
                    QueryBuilders.matchAllQuery(), filter));

        {
            final AggregationBuilder<?> terms = AggregationBuilders
                    .terms("terms").field(ElasticSearchMetadataBackend.KEY)
                    .size(0);
            request.addAggregation(terms);
        }

        final SearchResponse response = request.get();

        final Terms terms = (Terms) response.getAggregations().get("terms");

        final Set<String> keys = new HashSet<String>();

        for (final Terms.Bucket bucket : terms.getBuckets()) {
            keys.add(bucket.getKey());
        }

        return new FindKeys(keys, keys.size());
    }
}
