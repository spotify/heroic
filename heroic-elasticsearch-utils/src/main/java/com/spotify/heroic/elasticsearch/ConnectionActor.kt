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

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Receive
import com.spotify.heroic.elasticsearch.index.IndexMapping
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.ListenableActionFuture
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchScrollRequestBuilder
import org.elasticsearch.common.xcontent.XContentBuilder

class ConnectionActor(
    val index: IndexMapping,
    val client: ClientSetup.ClientWrapper,
    val templateName: String,
    val type: BackendType,
    context: ActorContext<Command>
): AbstractBehavior<ConnectionActor.Command>(context) {
    override fun createReceive(): Receive<Command> {
        return newReceiveBuilder()
            .onMessage(Command.Close::class.java, ::close)
            .onMessage(Command.Configure::class.java, ::configure)
            .onMessage(Command.ReadIndices::class.java) {
                it.replyTo.tell(Response.Indices(index.readIndices()))
                this
            }
            .onMessage(Command.WriteIndices::class.java) {
                it.replyTo.tell(Response.Indices(index.writeIndices()))
                this
            }
            .onMessage(Command.Search::class.java) {
                val result = index.search(client.client, it.type)
                it.replyTo.tell(Response.Search(result))
                this
            }
            .onMessage(Command.Count::class.java) {
                val result = index.count(client.client, it.type)
                it.replyTo.tell(Response.Search(result))
                this
            }
            .onMessage(Command.Index::class.java, ::index)
            .onMessage(Command.PrepareSearchScroll::class.java) {
                val result = client.client.prepareSearchScroll(it.scrollId)
                it.replyTo.tell(Response.SearchScroll(result))
                this
            }
            .onMessage(Command.Delete::class.java) {
                val result = index.delete(client.client, it.type, it.id)
                it.replyTo.tell(Response.Delete(result))
                this
            }
            .onAnyMessage {
                context.log.error("Received unknown message: {}", it)
                this
            }
            .build()
    }

    fun close(command: Command.Close): Behavior<Command> {
        client.shutdown.run()
        return this
    }

    fun configure(command: Command.Configure): Behavior<Command> {
        val indices = client.client.admin().indices()
        context.log.info("[{}] updating template for {}", templateName, index.template())

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

        command.replyTo.tell(Response.Configure(target.get()))
        return this
    }

    fun index(request: Command.Index): Behavior<Command> {
        val response = client.client.prepareIndex(request.index, request.type)
            .setId(request.id)
            .setSource(request.source)
            .setOpType(request.opType)
            .execute()
        request.replyTo.tell(Response.Index(response))
        return this
    }

    interface Command {
        data class Close(val replyTo: ActorRef<Response>): Command
        data class Configure(val replyTo: ActorRef<Response>): Command
        data class ReadIndices(val replyTo: ActorRef<Response>): Command
        data class WriteIndices(val replyTo: ActorRef<Response>): Command

        data class Search(val type: String, val replyTo: ActorRef<Response>): Command
        data class Count(val type: String, val replyTo: ActorRef<Response>): Command
        data class Index(
            val index: String,
            val type: String,
            val id: String,
            val source: XContentBuilder,
            val opType: DocWriteRequest.OpType,
            val replyTo: ActorRef<Response>
        ): Command
        data class PrepareSearchScroll(val scrollId: String, val replyTo: ActorRef<Response>): Command
        data class Delete(val type: String, val id: String, val replyTo: ActorRef<Response>): Command
    }

    interface Response {
        data class Configure(val response: PutIndexTemplateResponse): Response
        data class Indices(val indices: Array<String>): Response
        data class Search(val builder: SearchRequestBuilder): Response
        data class Index(val future: ListenableActionFuture<IndexResponse>): Response
        data class SearchScroll(val builder: SearchScrollRequestBuilder): Response
        data class Delete(val builders: List<DeleteRequestBuilder>): Response
    }
}
