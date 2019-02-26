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

import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;

/**
 * Common connection abstraction between Node and TransportClient.
 */
@Slf4j
@ToString(of = {"index", "client"})
public class Connection {
    private final AsyncFramework async;
    private final IndexMapping index;
    private final ClientSetup.ClientWrapper client;

    private final String templateName;
    private final BackendType type;

    @java.beans.ConstructorProperties({ "async", "index", "client", "templateName", "type" })
    public Connection(final AsyncFramework async, final IndexMapping index,
                      final ClientSetup.ClientWrapper client, final String templateName,
                      final BackendType type) {
        this.async = async;
        this.index = index;
        this.client = client;
        this.templateName = templateName;
        this.type = type;
    }

    public AsyncFuture<Void> close() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        futures.add(async.call((Callable<Void>) () -> {
            client.getShutdown().run();
            return null;
        }));

        return async.collectAndDiscard(futures);
    }

    public AsyncFuture<Void> configure() {
        final IndicesAdminClient indices = client.getClient().admin().indices();

        log.info("[{}] updating template for {}", templateName, index.template());

        final PutIndexTemplateRequestBuilder put = indices.preparePutTemplate(templateName);

        put.setSettings(type.getSettings());
        put.setTemplate(index.template());

        for (final Map.Entry<String, Map<String, Object>> mapping : type.getMappings().entrySet()) {
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
            public void onFailure(Exception e) {
                future.fail(e);
            }
        });

        future.onCancelled(() -> target.cancel(false));
        return future;
    }

    public String[] readIndices() throws NoIndexSelectedException {
        return index.readIndices();
    }

    public String[] writeIndices() throws NoIndexSelectedException {
        return index.writeIndices();
    }

    public SearchRequestBuilder search(String type) throws NoIndexSelectedException {
        return index.search(client.getClient(), type);
    }

    public SearchRequestBuilder count(String type) throws NoIndexSelectedException {
        return index.count(client.getClient(), type);
    }

    public IndexRequestBuilder index(String index, String type) {
        return client.getClient().prepareIndex(index, type);
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return client.getClient().prepareSearchScroll(scrollId);
    }

    public List<DeleteRequestBuilder> delete(String type, String id)
        throws NoIndexSelectedException {
        return index.delete(client.getClient(), type, id);
    }
}
