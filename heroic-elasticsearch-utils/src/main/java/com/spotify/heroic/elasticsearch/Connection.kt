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

import com.spotify.heroic.elasticsearch.index.NoIndexSelectedException
import eu.toolchain.async.AsyncFuture
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.search.ClearScrollRequestBuilder
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchScrollRequestBuilder

interface Connection {
    fun close(): AsyncFuture<Void?>

    fun configure(): AsyncFuture<Void?>

    @Throws(NoIndexSelectedException::class)
    fun readIndices(type: String): Array<String>

    @Throws(NoIndexSelectedException::class)
    fun writeIndices(type: String): Array<String>

    @Throws(NoIndexSelectedException::class)
    fun search(type: String): SearchRequestBuilder

    @Throws(NoIndexSelectedException::class)
    fun count(type: String): SearchRequestBuilder

    fun index(index: String, type: String): IndexRequestBuilder

    fun prepareSearchScroll(scrollId: String): SearchScrollRequestBuilder

    fun clearSearchScroll(scrollId: String): ClearScrollRequestBuilder

    fun prepareBulkRequest(): BulkRequestBuilder

    @Throws(NoIndexSelectedException::class)
    fun delete(type: String, id: String): List<DeleteRequestBuilder>
}