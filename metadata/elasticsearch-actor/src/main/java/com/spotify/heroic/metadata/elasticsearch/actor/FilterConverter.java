/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.metadata.elasticsearch.actor;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.TrueFilter;
import java.util.List;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

class FilterConverter implements Filter.Visitor<QueryBuilder> {
    private static final String TAG_KEYS = "tag_keys";
    private final String key;
    private final String tags;
    private final Character tag_delimiter;

    FilterConverter(String key, String tags, Character tag_delimiter) {
        this.key = key;
        this.tags = tags;
        this.tag_delimiter = tag_delimiter;
    }

    @Override
    public QueryBuilder visitTrue(final TrueFilter t) {
        return matchAllQuery();
    }

    @Override
    public QueryBuilder visitFalse(final FalseFilter f) {
        return new BoolQueryBuilder().mustNot(matchAllQuery());
    }

    @Override
    public QueryBuilder visitAnd(final AndFilter and) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (QueryBuilder qb : convertTerms(and.terms())) {
            boolQuery.must(qb);
        }
        return boolQuery;
    }

    @Override
    public QueryBuilder visitOr(final OrFilter or) {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        for (QueryBuilder qb : convertTerms(or.terms())) {
            boolQuery.should(qb);
        }
        boolQuery.minimumShouldMatch(1);
        return boolQuery;
    }

    @Override
    public QueryBuilder visitNot(final NotFilter not) {
        return new BoolQueryBuilder().mustNot(not.filter().visit(this));
    }

    @Override
    public QueryBuilder visitMatchTag(final MatchTagFilter matchTag) {
        return termQuery(tags, matchTag.tag() + tag_delimiter + matchTag.value());
    }

    @Override
    public QueryBuilder visitStartsWith(final StartsWithFilter startsWith) {
        return prefixQuery(tags,
            startsWith.tag() + tag_delimiter + startsWith.value());
    }

    @Override
    public QueryBuilder visitHasTag(final HasTagFilter hasTag) {
        return termQuery(TAG_KEYS, hasTag.tag());
    }

    @Override
    public QueryBuilder visitMatchKey(final MatchKeyFilter matchKey) {
        return termQuery(key, matchKey.key());
    }

    @Override
    public QueryBuilder defaultAction(final Filter filter) {
        throw new IllegalArgumentException("Unsupported filter statement: " + filter);
    }

    private QueryBuilder[] convertTerms(final List<Filter> terms) {
        final QueryBuilder[] filters = new QueryBuilder[terms.size()];
        int i = 0;

        for (final Filter stmt : terms) {
            filters[i++] = stmt.visit(this);
        }

        return filters;
    }
}
