package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.HashSet;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metadata.model.FindKeys;

@RequiredArgsConstructor
public class FindKeysResolver implements Callback.Resolver<FindKeys> {
    private final Client client;
    private final String index;
    private final String type;
    private final QueryBuilder query;

    @Override
    public FindKeys resolve() throws Exception {
        final SearchRequestBuilder request = client.prepareSearch(index)
                .setTypes(type).setSearchType("count");

        if (query != null) {
            request.setQuery(query);
        }

        {
            final AggregationBuilder<?> terms = AggregationBuilders.terms(
                    "terms").field(ElasticSearchMetadataBackend.TAGS_KEY);
            final AggregationBuilder<?> nested = AggregationBuilders
                    .nested("nested").path(ElasticSearchMetadataBackend.TAGS)
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

        return new FindKeys(keys, keys.size());
    }
}
