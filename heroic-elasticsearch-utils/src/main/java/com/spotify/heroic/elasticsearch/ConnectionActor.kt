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

package com.spotify.heroic.elasticsearch

import akka.actor.AbstractActor
import akka.event.Logging
import akka.event.LoggingAdapter
import com.spotify.heroic.elasticsearch.index.IndexMapping
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.ListenableActionFuture
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.common.xcontent.XContentBuilder

class ConnectionActor(
    val index: IndexMapping,
    val client: ClientSetup.ClientWrapper,
    val templateName: String,
    val type: BackendType
): AbstractActor() {
    private val log: LoggingAdapter = Logging.getLogger(context.system, this)

    override fun createReceive(): Receive {
        return receiveBuilder()
            .matchEquals(Message.CLOSE, { client.shutdown.run() })
            .matchEquals(Message.CONFIGURE, { configure() })
            .matchEquals(Message.READ_INDICES, { sender.tell(index.readIndices(), self) })
            .matchEquals(Message.WRITE_INDICES, { sender.tell(index.writeIndices(), self) })
            .match(Message.Search::class.java) {
                val result = index.search(client.client, it.type)
                sender.tell(result, self)
            }
            .match(Message.Count::class.java) {
                val result = index.count(client.client, it.type)
                sender.tell(result, self)
            }
            .match(Message.Index::class.java) {
                sender.tell(index(it), self)
            }
            .match(Message.PrepareSearchScroll::class.java) {
                val result = client.client.prepareSearchScroll(it.scrollId)
                sender.tell(result, self)
            }
            .match(Message.Delete::class.java) {
                val result = index.delete(client.client, it.type, it.id)
                sender.tell(result, self)
            }
            .matchAny { log.error("Received unknown message: {}", it) }
            .build()
    }

    fun configure(): PutIndexTemplateResponse {
        val indices = client.client.admin().indices()
        log.info("[{}] updating template for {}", templateName, index.template())

        val put = indices.preparePutTemplate(templateName)
            .setSettings(type.settings)
            .setTemplate(index.template())
        type.mappings?.forEach { (key, value) -> put.addMapping(key, value) }

        val target = put.execute()
        target.addListener(object: ActionListener<PutIndexTemplateResponse> {
            override fun onResponse(response: PutIndexTemplateResponse) {
                if (!response.isAcknowledged) {
                    onFailure(Exception("request not acknowledged"))
                }
            }

            override fun onFailure(e: Exception) {
                throw e
            }
        })
        return target.get()
    }

    fun index(request: Message.Index): ListenableActionFuture<IndexResponse> {
        return client.client.prepareIndex(request.index, request.type)
            .setId(request.id)
            .setSource(request.source)
            .setOpType(request.opType)
            .execute()
    }

    enum class Message {
        CLOSE, CONFIGURE, READ_INDICES, WRITE_INDICES;

        data class Search(val type: String)
        data class Count(val type: String)
        data class Index(
            val index: String,
            val type: String,
            val id: String,
            val source: XContentBuilder,
            val opType: DocWriteRequest.OpType
        )
        data class PrepareSearchScroll(val scrollId: String)
        data class Delete(val type: String, val id: String)
    }
}
