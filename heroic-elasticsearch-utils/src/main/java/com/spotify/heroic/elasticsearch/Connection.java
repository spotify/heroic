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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Common connection abstraction between Node and TransportClient.
 */
@Slf4j
@RequiredArgsConstructor
@ToString(of = { "index", "client" })
public class Connection {
    private final AsyncFramework async;
    private final IndexMapping index;
    private final Client client;
    private final BulkProcessor bulk;

    private final String templateName;
    private final Map<String, Map<String, Object>> mappings;
    private final Map<String, Object> settings;

    public AsyncFuture<Void> close() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        futures.add(async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                client.close();
                return null;
            }
        }));

        futures.add(async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                bulk.close();
                return null;
            }
        }));

        return async.collectAndDiscard(futures);
    }

    public AsyncFuture<Void> configure() {
        final IndicesAdminClient indices = client.admin().indices();

        log.info("[{}] updating template for {}", templateName, index.template());

        final PutIndexTemplateRequestBuilder put = indices.preparePutTemplate(templateName);

        put.setSettings(settings);
        put.setTemplate(index.template());

        for (final Map.Entry<String, Map<String, Object>> mapping : mappings.entrySet()) {
            put.addMapping(mapping.getKey(), mapping.getValue());
        }

        final ResolvableFuture<Void> future = async.future();

        final ListenableActionFuture<PutIndexTemplateResponse> target = put.execute();

        target.addListener(new ActionListener<PutIndexTemplateResponse>() {
            @Override
            public void onResponse(final PutIndexTemplateResponse response) {
                if (!response.isAcknowledged()) {
                    future.fail(new Exception("request not acknowledged"));
                    return;
                }

                future.resolve(null);
            }

            @Override
            public void onFailure(Throwable e) {
                future.fail(e);
            }
        });

        future.onCancelled(() -> target.cancel(false));
        return future;
    }

    public String[] readIndices(DateRange range) throws NoIndexSelectedException {
        return index.readIndices(range);
    }

    public String[] writeIndices(DateRange range) throws NoIndexSelectedException {
        return index.writeIndices(range);
    }

    public SearchRequestBuilder search(DateRange range, String type)
            throws NoIndexSelectedException {
        return index.search(client, range, type);
    }

    public CountRequestBuilder count(DateRange range, String type) throws NoIndexSelectedException {
        return index.count(client, range, type);
    }

    public DeleteByQueryRequestBuilder deleteByQuery(DateRange range, String type)
            throws NoIndexSelectedException {
        return index.deleteByQuery(client, range, type);
    }

    public IndexRequestBuilder index(String index, String type) {
        return client.prepareIndex(index, type);
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return client.prepareSearchScroll(scrollId);
    }

    public Client client() {
        return client;
    }

    public BulkProcessor bulk() {
        return bulk;
    }
}
