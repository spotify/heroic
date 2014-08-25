package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metadata.elasticsearch.ElasticSearchMetadataBackend;
import com.spotify.heroic.metadata.model.FindTags;

@RequiredArgsConstructor
public class FindTagsResolver implements Callback.Resolver<FindTags> {
    private final Client client;
    private final String index;
    private final String type;
    private final FilterBuilder filter;
    private final String key;

    @Override
    public FindTags resolve() throws Exception {
        final SearchRequestBuilder request = client.prepareSearch(index)
                .setTypes(type).setSearchType("count").setSize(0);

        if (filter != null)
            request.setQuery(QueryBuilders.filteredQuery(
                    QueryBuilders.matchAllQuery(), filter));

        {
            final AggregationBuilder<?> terms = AggregationBuilders
                    .terms("terms")
                    .field(ElasticSearchMetadataBackend.TAGS_VALUE).size(0);
            final AggregationBuilder<?> filter = AggregationBuilders
                    .filter("filter")
                    .filter(FilterBuilders.termFilter(
                            ElasticSearchMetadataBackend.TAGS_KEY, key))
                    .subAggregation(terms);
            final AggregationBuilder<?> aggregation = AggregationBuilders
                    .nested("nested").path(ElasticSearchMetadataBackend.TAGS)
                    .subAggregation(filter);
            request.addAggregation(aggregation);
        }

        final SearchResponse response = request.get();

        final Terms terms;

        /*
         * IMPORTANT: has to be unwrapped with the correct type in the correct
         * order as specified above!
         */
        {
            final Aggregations aggregations = response.getAggregations();
            final Nested tags = (Nested) aggregations.get("nested");
            final Filter filter = (Filter) tags.getAggregations().get("filter");
            terms = (Terms) filter.getAggregations().get("terms");
        }

        final Set<String> values = new HashSet<String>();

        for (final Terms.Bucket bucket : terms.getBuckets()) {
            values.add(bucket.getKey());
        }

        final Map<String, Set<String>> result = new HashMap<String, Set<String>>();
        result.put(key, values);
        return new FindTags(result, result.size());
    }
}
