/*
 * Copyright (c) 2020 Spotify AB.
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

package com.spotify.heroic.elasticsearch

import com.spotify.heroic.elasticsearch.index.IndexMapping
import eu.toolchain.async.AsyncFuture
import org.elasticsearch.action.ActionFuture
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.ClearScrollResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.Aggregation
import java.util.stream.Stream

interface Connection<in T: Aggregation> {
    val index: IndexMapping

    fun close(): AsyncFuture<Void?>

    fun configure(): AsyncFuture<Void?>

    fun searchScroll(scrollId: String, timeout: TimeValue, listener: ActionListener<SearchResponse>)

    fun clearSearchScroll(scrollId: String): ActionFuture<ClearScrollResponse>

    /**
     * Synchronously execute a search request.
     */
    fun execute(request: SearchRequest): SearchResponse

    /**
     * Execute a search request asynchronously and pass the result to the listener.
     */
    fun execute(request: SearchRequest, listener: ActionListener<SearchResponse>)

    /**
     * Execute a delete request asynchronously and pass the result to the listener.
     */
    fun execute(request: DeleteRequest, listener: ActionListener<DeleteResponse>)

    /**
     * Execute a bulk request asynchronously and pass the result to the listener.
     */
    fun execute(request: BulkRequest, listener: ActionListener<BulkResponse>)

    /**
     * Execute an index request asynchronously and pass the result to the listener.
     */
    fun execute(request: IndexRequest, listener: ActionListener<IndexResponse>)

    /**
     * Temporary helper to handle different classes returned from the REST and transport clients.
     * TODO: Remove once the transport client is removed.
     */
    fun parseHits(terms: T): Stream<Pair<String, SearchHits>>
}