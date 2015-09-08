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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
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
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
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
        return async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                configureMapping(client);
                return null;
            }
        });
    }

    public String[] readIndices(DateRange range) throws NoIndexSelectedException {
        return index.readIndices(range);
    }

    public String[] writeIndices(DateRange range) throws NoIndexSelectedException {
        return index.writeIndices(range);
    }

    public SearchRequestBuilder search(DateRange range, String type) throws NoIndexSelectedException {
        return index.search(client, range, type);
    }

    public CountRequestBuilder count(DateRange range, String type) throws NoIndexSelectedException {
        return index.count(client, range, type);
    }

    public DeleteByQueryRequestBuilder deleteByQuery(DateRange range, String type) throws NoIndexSelectedException {
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

    private void configureMapping(Client client)
            throws Exception {
        final IndicesAdminClient indices = client.admin().indices();

        if (isTemplateUpToDate(indices)) {
            log.info("[{}] template up-to-date", templateName);
            return;
        }

        log.info("[{}] updating template", templateName);
        updateTemplate(indices);
    }

    private boolean isTemplateUpToDate(IndicesAdminClient indices) throws Exception {
        final GetIndexTemplatesResponse response = indices
                .getTemplates(indices.prepareGetTemplates(templateName).request()).get(30, TimeUnit.SECONDS);

        for (final IndexTemplateMetaData t : response.getIndexTemplates()) {
            if (t.getName().equals(templateName)) {
                return compareTemplate(t);
            }
        }

        return false;
    }

    private boolean compareTemplate(final IndexTemplateMetaData t) throws IOException {
        if (t.getTemplate() == null) {
            log.warn("{}.template (missing)", t.getName());
            return false;
        }

        if (!t.getTemplate().equals(index.template())) {
            log.warn("{}.template\nactual: {}\nwanted:{}", t.getName(), t.getTemplate(), index.template());
            return false;
        }

        final ImmutableOpenMap<String, CompressedString> externalMappings = t.getMappings();

        if (externalMappings == null || externalMappings.isEmpty()) {
            log.warn("{}.mappings (empty)", t.getName());
            return false;
        }

        if (mappings.size() != externalMappings.size()) {
            log.warn("{}.mappings: size differ, {} (actual) != {} (wanted)", t.getName(), externalMappings.size(),
                    mappings.size());
        }

        for (final Map.Entry<String, Map<String, Object>> mapping : mappings.entrySet()) {
            final CompressedString external = externalMappings.get(mapping.getKey());

            if (external == null) {
                log.warn("{}.mappings.{} (missing)", t.getName(), mapping.getKey());
                return false;
            }

            final Map<String, Object> e = JsonXContent.jsonXContent.createParser(external.string()).map();

            e.get(mapping.getKey());

            if (!e.equals(mapping.getValue())) {
                log.warn("{}.mappings.{}:\nactual: {}\nwanted: {}", t.getName(), mapping.getKey(), e,
                        mapping.getValue());
                return false;
            }
        }

        return true;
    }

    private void updateTemplate(final IndicesAdminClient indices) throws Exception {
        final PutIndexTemplateRequestBuilder put = indices.preparePutTemplate(templateName);

        put.setSettings(settings);
        put.setTemplate(index.template());

        for (final Map.Entry<String, Map<String, Object>> mapping : mappings.entrySet()) {
            put.addMapping(mapping.getKey(), mapping.getValue());
        }

        final PutIndexTemplateResponse response = put.get();

        if (!response.isAcknowledged()) {
            throw new Exception("Failed to setup mapping: " + response.toString());
        }
    }
}
