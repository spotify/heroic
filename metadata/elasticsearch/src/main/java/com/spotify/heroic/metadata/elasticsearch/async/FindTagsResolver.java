/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metadata.elasticsearch.async;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import lombok.RequiredArgsConstructor;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.spotify.heroic.elasticsearch.ElasticsearchUtils;
import com.spotify.heroic.metadata.model.FindTags;

@RequiredArgsConstructor
public class FindTagsResolver implements Callable<FindTags> {
    private final Callable<SearchRequestBuilder> setup;
    private final ElasticsearchUtils.FilterContext ctx;
    private final FilterBuilder filter;
    private final String key;

    @Override
    public FindTags call() throws Exception {
        final SearchRequestBuilder request = setup.call().setSearchType("count").setSize(0);

        request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter));

        {
            final AggregationBuilder<?> terms = AggregationBuilders.terms("terms").field(ctx.tagsValue()).size(0);
            final AggregationBuilder<?> filter = AggregationBuilders.filter("filter")
                    .filter(FilterBuilders.termFilter(ctx.tagsKey(), key)).subAggregation(terms);
            final AggregationBuilder<?> aggregation = AggregationBuilders.nested("nested").path(ctx.tags())
                    .subAggregation(filter);
            request.addAggregation(aggregation);
        }

        final SearchResponse response = request.get();

        final Terms terms;

        /* IMPORTANT: has to be unwrapped with the correct type in the correct order as specified above! */
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
