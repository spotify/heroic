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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.elasticsearch.AbstractElasticsearchMetadataBackend.SearchTransform;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class SearchTransformTest {
    private final String scrollID = "scroller1";

    @Mock
    private AsyncFramework async;

    @Mock
    private AsyncFuture<SearchTransformResult> resolved;

    @Mock
    Supplier<AsyncFuture<SearchResponse>> scroller;

    @Mock
    BiFunction<String, SearchHit, Supplier<AsyncFuture<SearchResponse>>> scrollerFactory;

    @Mock
    AsyncFuture<SearchResponse> response;

    @Mock
    SearchResponse searchResponse;

    @Mock
    SearchHits searchHits;

    private final SearchHit[] searchHits1 = {
        mock(SearchHit.class), mock(SearchHit.class), mock(SearchHit.class),
    };

    private final SearchHit[] searchHits2 = {
        mock(SearchHit.class), mock(SearchHit.class), mock(SearchHit.class),
    };

    private final SearchHit[] emptySearchHits = {};

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        doReturn(resolved).when(async).resolved(any(SearchTransformResult.class));
        doReturn(searchHits).when(searchResponse).getHits();
        doReturn(scrollID).when(searchResponse).getScrollId();

        doReturn(scroller).when(scrollerFactory).apply(any(String.class), any(SearchHit.class));
        doReturn(response).when(scroller).get();
        doAnswer((Answer<AsyncFuture<SearchTransformResult<Integer>>>) invocation -> {
            final LazyTransform<SearchResponse, SearchTransformResult<Integer>> transform =
                (LazyTransform<SearchResponse, SearchTransformResult<Integer>>)
                    invocation.getArguments()[0];
            return transform.transform(searchResponse);
        }).when(response).lazyTransform(any(LazyTransform.class));
    }

    public void setSearchHitPages(SearchHit[]... pages) {
        OngoingStubbing<SearchHit[]> stub = when(searchHits.getHits());

        for (final SearchHit[] page : pages) {
            stub = stub.thenReturn(page);
        }
    }

    public SearchTransform<SearchHit> createScrollTransform(Integer limit) {
        final OptionalLimit optionalLimit = OptionalLimit.of(limit);
        return new SearchTransform<>(
            async, optionalLimit, Function.identity(), scrollerFactory, true);
    }

    public SearchTransformResult<SearchHit> createLimitSet(Integer limit, SearchHit[]... pages) {
        final Set<SearchHit> set = new HashSet<>();

        Stream<SearchHit> stream =
            Arrays.stream(pages).map(Arrays::stream).reduce(Stream.empty(), Stream::concat);

        if (limit != null) {
            stream = stream.limit(limit);
        }

        stream.forEach(set::add);

        return new SearchTransformResult<>(set, limit != null, scrollID);
    }

    @Test
    public void aboveLimit() {
        setSearchHitPages(searchHits1, emptySearchHits);
        final Integer limit = searchHits1.length - 1;

        final SearchTransform<SearchHit> searchTransform = createScrollTransform(limit);

        scroller.get().lazyTransform(searchTransform);

        verify(async).resolved(createLimitSet(limit, searchHits1));
        verify(scroller).get();
    }

    @Test
    public void aboveLimitWithDuplicates() {
        setSearchHitPages(searchHits1, searchHits1, searchHits2, emptySearchHits);
        final Integer limit = searchHits1.length + searchHits2.length - 1;

        final SearchTransform<SearchHit> searchTransform = createScrollTransform(limit);

        scroller.get().lazyTransform(searchTransform);

        verify(async).resolved(createLimitSet(limit, searchHits1, searchHits2));
        verify(scroller, times(3)).get();
    }

    @Test
    public void belowLimit() {
        setSearchHitPages(searchHits1, emptySearchHits);
        final Integer limit = searchHits1.length;

        final SearchTransform<SearchHit> searchTransform = createScrollTransform(limit);

        scroller.get().lazyTransform(searchTransform);

        verify(async).resolved(createLimitSet(null, searchHits1));
        verify(scroller, times(2)).get();
    }

    @Test
    public void belowLimitWithDuplicates() {
        setSearchHitPages(searchHits1, searchHits1, searchHits2, emptySearchHits);
        final Integer limit = searchHits1.length + searchHits2.length;

        final SearchTransform<SearchHit> searchTransform = createScrollTransform(limit);

        scroller.get().lazyTransform(searchTransform);

        verify(async).resolved(createLimitSet(null, searchHits1, searchHits2));
        verify(scroller, times(4)).get();
    }
}
