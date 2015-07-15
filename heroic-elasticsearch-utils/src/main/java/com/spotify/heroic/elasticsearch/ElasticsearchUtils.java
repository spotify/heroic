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

package com.spotify.heroic.elasticsearch;

import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.boolFilter;
import static org.elasticsearch.index.query.FilterBuilders.matchAllFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.notFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.prefixFilter;
import static org.elasticsearch.index.query.FilterBuilders.regexpFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.Series;

public final class ElasticsearchUtils {
    public static final String TYPE_METADATA = "metadata";
    public static final String TYPE_TAG = "tag";
    public static final String TYPE_SERIES = "series";

    /**
     * Fields for type "series".
     **/
    public static final String SERIES_KEY = "key";
    public static final String SERIES_KEY_RAW = "key.raw";
    public static final String SERIES_TAGS = "tags";

    /**
     * Fields for type "metadata".
     */
    public static final String METADATA_KEY = "key";
    public static final String METADATA_TAGS = "tags";

    /**
     * Fields for type "tag".
     */
    public static final String TAG_KEY = "key";
    public static final String TAG_KEY_RAW = "key.raw";
    public static final String TAG_VALUE = "value";
    public static final String TAG_VALUE_RAW = "value.raw";
    public static final String TAG_KV = "kv";
    public static final String TAG_SERIES = "series";

    /**
     * common fields, but nested in different ways depending on document type.
     *
     * @see FilterContext
     */
    public static final String KEY = "key";
    public static final String KEY_RAW = "key.raw";
    public static final String TAGS = "tags";
    public static final String TAGS_KEY = "key";
    public static final String TAGS_KEY_RAW = "key.raw";
    public static final String TAGS_VALUE = "value";
    public static final String TAGS_VALUE_RAW = "value.raw";
    public static final String ID = "id";

    @SuppressWarnings("unchecked")
    public static Series toSeries(Map<String, Object> source) {
        final String key = (String) source.get("key");
        final Map<String, String> tags = toTags((List<Map<String, String>>) source.get("tags"));
        return new Series(key, tags);
    }

    public static Map<String, String> toTags(final List<Map<String, String>> source) {
        final Map<String, String> tags = new HashMap<String, String>();

        for (final Map<String, String> entry : source) {
            final String key = entry.get("key");
            final String value = entry.get("value");
            tags.put(key, value);
        }

        return tags;
    }

    public static void buildMetadataDoc(final XContentBuilder b, Series series) throws IOException {
        b.field(METADATA_KEY, series.getKey());

        b.startArray(METADATA_TAGS);

        if (series.getTags() != null && !series.getTags().isEmpty()) {
            for (final Map.Entry<String, String> entry : series.getTags().entrySet()) {
                b.startObject();
                b.field(TAGS_KEY, entry.getKey());
                b.field(TAGS_VALUE, entry.getValue());
                b.endObject();
            }
        }

        b.endArray();
    }

    public static void buildTagDoc(final XContentBuilder b, BytesReference series, Entry<String, String> e)
            throws IOException {
        b.rawField(TAG_SERIES, series);
        b.field(TAG_KEY, e.getKey());
        b.field(TAG_VALUE, e.getValue());
        b.field(TAG_KV, e.getKey() + "\t" + e.getValue());
    }

    public static List<String> tokenize(Analyzer analyzer, String field, String keywords) throws IOException {
        final List<String> terms = new ArrayList<String>();

        try (final Reader reader = new StringReader(keywords)) {
            try (final TokenStream stream = analyzer.tokenStream(field, reader)) {
                final CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);

                stream.reset();

                final String first = term.toString();

                if (!first.isEmpty())
                    terms.add(first);

                while (stream.incrementToken()) {
                    final String next = term.toString();

                    if (next.isEmpty())
                        continue;

                    terms.add(next);
                }

                stream.end();
            }
        }

        return terms;
    }

    public static FilterContext context(String... path) {
        return new FilterContext(path);
    }

    public static final class FilterContext {
        private final String seriesKey;
        private final String tags;
        private final String seriesId;
        private final String tagsKey;
        private final String tagsValue;

        private FilterContext(String... path) {
            this(ImmutableList.<String> builder().add(path).build());
        }

        private FilterContext(List<String> path) {
            this.seriesKey = path(path, KEY);
            this.tags = path(path, TAGS);
            this.seriesId = path(path, ID);
            this.tagsKey = path(path, TAGS, TAGS_KEY_RAW);
            this.tagsValue = path(path, TAGS, TAGS_VALUE_RAW);
        }

        private String path(List<String> path, String tail) {
            return StringUtils.join(ImmutableList.builder().addAll(path).add(tail).build(), '.');
        }

        private String path(List<String> path, String tailN, String tail) {
            return StringUtils.join(ImmutableList.builder().addAll(path).add(tailN).add(tail).build(), '.');
        }

        public String seriesKey() {
            return seriesKey;
        }

        public String tags() {
            return tags;
        }

        public String tagsKey() {
            return tagsKey;
        }

        public String tagsValue() {
            return tagsValue;
        }

        public String seriesId() {
            return seriesId;
        }

        public FilterBuilder filter(final Filter filter) {
            if (filter instanceof Filter.True)
                return matchAllFilter();

            if (filter instanceof Filter.False)
                return null;

            if (filter instanceof Filter.And) {
                final Filter.And and = (Filter.And) filter;
                final List<FilterBuilder> filters = new ArrayList<>(and.terms().size());

                for (final Filter stmt : and.terms())
                    filters.add(filter(stmt));

                return andFilter(filters.toArray(new FilterBuilder[0]));
            }

            if (filter instanceof Filter.Or) {
                final Filter.Or or = (Filter.Or) filter;
                final List<FilterBuilder> filters = new ArrayList<>(or.terms().size());

                for (final Filter stmt : or.terms())
                    filters.add(filter(stmt));

                return orFilter(filters.toArray(new FilterBuilder[0]));
            }

            if (filter instanceof Filter.Not) {
                final Filter.Not not = (Filter.Not) filter;
                return notFilter(filter(not.first()));
            }

            if (filter instanceof Filter.MatchTag) {
                final Filter.MatchTag matchTag = (Filter.MatchTag) filter;

                final BoolFilterBuilder nested = boolFilter();
                nested.must(termFilter(tagsKey, matchTag.first()));
                nested.must(termFilter(tagsValue, matchTag.second()));
                return nestedFilter(tags, nested);
            }

            if (filter instanceof Filter.StartsWith) {
                final Filter.StartsWith startsWith = (Filter.StartsWith) filter;

                final BoolFilterBuilder nested = boolFilter();
                nested.must(termFilter(tagsKey, startsWith.first()));
                nested.must(prefixFilter(tagsValue, startsWith.second()));
                return nestedFilter(tags, nested);
            }

            if (filter instanceof Filter.Regex) {
                final Filter.Regex regex = (Filter.Regex) filter;

                final BoolFilterBuilder nested = boolFilter();
                nested.must(termFilter(tagsKey, regex.first()));
                nested.must(regexpFilter(tagsValue, regex.second()));
                return nestedFilter(tags, nested);
            }

            if (filter instanceof Filter.HasTag) {
                final Filter.HasTag hasTag = (Filter.HasTag) filter;
                final TermFilterBuilder nested = termFilter(tagsKey, hasTag.first());
                return nestedFilter(tags, nested);
            }

            if (filter instanceof Filter.MatchKey) {
                final Filter.MatchKey matchKey = (Filter.MatchKey) filter;
                return termFilter(seriesKey, matchKey.first());
            }

            throw new IllegalArgumentException("Invalid filter statement: " + filter);
        }
    }
}
