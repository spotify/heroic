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
import eu.toolchain.async.AsyncFramework
import eu.toolchain.async.AsyncFuture
import org.elasticsearch.action.ActionFuture
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.*
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue

class RestConnection(
    private val client: RestHighLevelClient,
    private val async: AsyncFramework,
    override val index: IndexMapping
): Connection {
    private val options = RequestOptions.DEFAULT

    override fun close(): AsyncFuture<Void?> {
        val future = async.call(client::close).directTransform { null }
        return async.collectAndDiscard(listOf(future))
    }

    override fun configure(): AsyncFuture<Void?> {
        TODO("not implemented")
    }

    override fun searchScroll(
        scrollId: String, timeout: TimeValue, listener: ActionListener<SearchResponse>
    ) {
        TODO("not implemented")
    }

    override fun clearSearchScroll(scrollId: String): ActionFuture<ClearScrollResponse> {
        TODO("not implemented")
    }

    override fun execute(request: SearchRequest): SearchResponse {
        return client.search(request, options)
    }

    override fun execute(request: SearchRequest, listener: ActionListener<SearchResponse>) {
        client.searchAsync(request, options, listener)
    }

    override fun execute(request: DeleteRequest, listener: ActionListener<DeleteResponse>) {
        client.deleteAsync(request, options, listener)
    }

    override fun execute(request: BulkRequest, listener: ActionListener<BulkResponse>) {
        client.bulkAsync(request, options, listener)
    }

    override fun execute(request: IndexRequest, listener: ActionListener<IndexResponse>) {
        client.indexAsync(request, options, listener)
    }
}