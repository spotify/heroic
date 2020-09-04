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
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.*
import org.elasticsearch.action.support.PlainActionFuture
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.PutIndexTemplateRequest
import org.elasticsearch.client.sniff.Sniffer
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.SearchHits
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms
import org.elasticsearch.search.aggregations.metrics.TopHits
import org.slf4j.LoggerFactory
import java.util.stream.Stream

private val logger = LoggerFactory.getLogger(RestConnection::class.java)

class RestConnection(
    private val client: RestHighLevelClient,
    private val sniffer: Sniffer?,
    private val async: AsyncFramework,
    override val index: IndexMapping,
    private val templateName: String,
    private val type: BackendType
): Connection<ParsedStringTerms> {
    private val options = RequestOptions.DEFAULT

    override fun close(): AsyncFuture<Void?> {
        val future = async.call {
            sniffer?.close()
            client.close()
        }
        return async.collectAndDiscard(listOf(future))
    }

    override fun configure(): AsyncFuture<Void?> {
        val writes = type.mappings.map { (indexType, map) ->
            val templateWithType = "$templateName-$indexType"
            val pattern = index.template.replace("\\*", "$indexType-*")

            logger.info("[{}] updating template for {}", templateWithType, pattern)

            val settings = type.settings.toMutableMap()
            settings["index"] = index.settings

            val request = PutIndexTemplateRequest(templateWithType)
                .settings(settings)
                .patterns(listOf(pattern))
                .mapping(map)
                .order(100)

            val future = async.future<AcknowledgedResponse>()
            val listener = object: ActionListener<AcknowledgedResponse> {
                override fun onFailure(e: Exception) {
                    @Suppress("DEPRECATION")
                    future.fail(e)
                }

                override fun onResponse(response: AcknowledgedResponse) {
                    if (!response.isAcknowledged) {
                        @Suppress("DEPRECATION")
                        future.fail(Exception("request not acknowledged"))
                    }
                    future.resolve(null)
                }
            }

            client.indices().putTemplateAsync(request, options, listener)
            future
        }

        return async.collectAndDiscard(writes)
    }

    override fun searchScroll(
        scrollId: String, timeout: TimeValue, listener: ActionListener<SearchResponse>
    ) {
        val request = SearchScrollRequest(scrollId).scroll(timeout)
        client.scrollAsync(request, options, listener)
    }

    override fun clearSearchScroll(scrollId: String): ActionFuture<ClearScrollResponse> {
        val request = ClearScrollRequest()
        request.addScrollId(scrollId)
        val future = PlainActionFuture.newFuture<ClearScrollResponse>()
        client.clearScrollAsync(request, options, future)
        return future
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

    override fun parseHits(terms: ParsedStringTerms): Stream<Pair<String, SearchHits>> {
        return terms.buckets
            .stream()
            .map {
                it.keyAsString to it.aggregations.get<TopHits>("hits").hits
            }
    }
}
