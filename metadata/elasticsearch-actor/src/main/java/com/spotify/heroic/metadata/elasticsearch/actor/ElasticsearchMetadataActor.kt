/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"): you may not use this file except in compliance
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

package com.spotify.heroic.metadata.elasticsearch.actor

import akka.actor.AbstractActor
import akka.actor.ActorRef
import akka.actor.Props
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.pattern.Patterns.ask
import com.spotify.heroic.elasticsearch.BackendType
import com.spotify.heroic.elasticsearch.ConnectionActor
import com.spotify.heroic.elasticsearch.ConnectionModule
import com.spotify.heroic.elasticsearch.ResourceLoader
import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException
import com.spotify.heroic.metadata.*
import com.spotify.heroic.statistics.MetadataBackendReporter
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.ListenableActionFuture
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import java.time.Duration
import java.util.concurrent.CompletionStage
import java.util.function.Consumer

class ElasticsearchMetadataActor(
    private val reporter: MetadataBackendReporter,
    private val connectionProps: Props,
    //private val connection: Connection,
    //private val writeCache: RateLimitedCache<Pair<String, HashCode>>,
    private val configure: Boolean,
    private val deleteParallelism: Int,
    private val connectionTimeout: Duration = Duration.ofSeconds(5)
): AbstractActor(), MetadataActor {
    private lateinit var connection: ActorRef

    // TODO: figure out how to inject writeCache

    // TODO: need to make an actor version of the SemanticStatistics module
    //override val statistics: Statistics = Statistics(WRITE_CACHE_SIZE, writeCache.size().toLong())

    // TODO: add tracing

    private val log: LoggingAdapter = Logging.getLogger(context.system, this)

    override fun preStart() {
        connection = context.actorOf(connectionProps, "connection")
    }


    override fun createReceive(): Receive {
        return receiveBuilder()
            .matchEquals("stop", {
                connection.tell(ConnectionActor.Message.CLOSE, sender)
            })
            .matchEquals("configure", {
                connection.tell(ConnectionActor.Message.CONFIGURE, sender)
            })
            .match(WriteMetadata.Request::class.java) { sender.tell(write(it), self) }
            .match(FindTags.Request::class.java) { sender.tell(findTags(it), self) }
            .match(FindSeries.Request::class.java) { sender.tell(findSeries(it), self) }
            .match(FindSeriesIds.Request::class.java) { sender.tell(findSeriesIds(it), self) }
            .match(CountSeries.Request::class.java) { sender.tell(countSeries(it), self) }
            .match(DeleteSeries.Request::class.java) { sender.tell(deleteSeries(it), self) }
            .match(FindKeys.Request::class.java) { sender.tell(findKeys(it), self) }
            .matchAny { log.error("Received unknown message: {}", it) }
            .build()
    }

    override fun write(request: WriteMetadata.Request): WriteMetadata {
        //TODO("not implemented")

        // TODO: how to handle exceptions in actor messages? this can throw NoIndexSelectedException

        val indices = ask(connection, ConnectionActor.Message.WRITE_INDICES, connectionTimeout)
            .toCompletableFuture()
            .join()
            as Array<String>

        val series = request.series
        val id = series.hash()

        val writes = indices
            /*
            .mapNotNull {
            if (!writeCache.acquire(it to series.hashCodeTagOnly,
                    reporter::reportWriteDroppedByCacheHit)) {
                null
            } else {
                it
            }
        }*/
        .map { index: String ->
            val source = XContentFactory.jsonBuilder()
            source.startObject()
            buildContext(source, series)
            source.endObject()

            val msg = ConnectionActor.Message.Index(
                index = index,
                type = TYPE_METADATA,
                id = id,
                source = source,
                opType = DocWriteRequest.OpType.CREATE
            )

            val timer = WriteMetadata.timer()
            val writeContext = reporter.setupBackendWriteReporter()

            val result = ask(connection, msg, connectionTimeout)
                .toCompletableFuture()
                .join()
                as ListenableActionFuture<IndexResponse>

            result.addListener(object: ActionListener<IndexResponse> {
                override fun onFailure(e: Exception) {
                    TODO("add handleVersionConflict")
                    writeContext.resolved(e)
                }

                override fun onResponse(response: IndexResponse) {
                    writeContext.resolved(timer.end())
                }

            })
            result
        }

        TODO("map the ListenableActionFutures into WriteMetadata and reduce the metadata")
    }

    override fun findTags(request: FindTags.Request): FindTags {
        log.info("Finding empty tags")
        return FindTags.EMPTY
    }

    override fun findSeries(request: FindSeries.Request): FindSeries {
        return entries(
            connection,
            request.filter,
            request.limit,
            ::toSeries,
            { FindSeries(it.set!!, it.isLimited) },
            Consumer { }
        )
    }

    override fun findSeriesIds(request: FindSeriesIds.Request): FindSeriesIds {
        return entries(
            connection,
            request.filter,
            request.limit,
            SearchHit::getId,
            { FindSeriesIds(it.set!!, it.isLimited) },
            Consumer { it.setFetchSource(false) }
        )
    }

    override fun countSeries(request: CountSeries.Request): CountSeries {
        if (request.limit.isZero) return CountSeries()

        val builder = connection.count(TYPE_METADATA)
        request.limit.asInteger().ifPresent { builder.setTerminateAfter(it) }

        builder.setQuery(BoolQueryBuilder().must(filter(request.filter)))
        builder.setSize(0)
        return CountSeries(builder.get().hits.totalHits, false)
    }

    override fun deleteSeries(request: DeleteSeries.Request): DeleteSeries {
        TODO("not implemeneted")

        val findIds = FindSeriesIds.Request(request.filter, request.range, request.limit)

        // TODO: take deleteParallelism into account and parallelize the deletions
        // maybe make Connection an actor?

        findSeriesIds(findIds).ids
            .flatMap { connection.delete(TYPE_METADATA, it) }
            .map { it.execute() }
    }

    override fun findKeys(request: FindKeys.Request): FindKeys {
        val response = connection.search(TYPE_METADATA)
            .setQuery(BoolQueryBuilder().must(filter(request.filter)))
            .addAggregation(AggregationBuilders.terms("terms").field(KEY))
            .get()

        val keys = mutableSetOf<String>()
        val terms: Terms = response.aggregations.get("terms")
        var duplicates = 0
        terms.buckets.forEach {
            if (keys.add(it.keyAsString)) {
                duplicates += 1
            }
        }

        return FindKeys(keys, terms.buckets.size, duplicates)
    }

    companion object {
        @JvmStatic
        fun backendType(): BackendType {
            val mappings = mapOf(
                "metadata" to ResourceLoader.loadJson(this::class.java,"metadata.json")
            )
            return BackendType(mappings, emptyMap(), this::class.java)
        }
    }
}
