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

package com.spotify.heroic.elasticsearch.index

import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.search.builder.SearchSourceBuilder

private const val DEFAULT_INDEX = "heroic"
private val DEFAULT_SETTINGS: Map<String, Any> = mapOf()

data class SingleIndexMapping(
    private val index: String = DEFAULT_INDEX,
    override val settings: Map<String, Any> = DEFAULT_SETTINGS
): IndexMapping {
    override val template = index

    override fun readIndices(type: String): Array<String> {
        return arrayOf(getFullIndexName(type))
    }

    override fun writeIndices(type: String): Array<String> {
        return arrayOf(getFullIndexName(type))
    }

    override fun search(type: String): SearchRequest {
        return SearchRequest(getFullIndexName(type))
    }

    override fun count(type: String): SearchRequest {
        return search(type).source(SearchSourceBuilder().size(0))
    }

    override fun delete(type: String, id: String): List<DeleteRequest> {
        return listOf(DeleteRequest(getFullIndexName(type), id))
    }

    private fun getFullIndexName(type: String) = "$index-$type"

    class Builder {
        var index: String = DEFAULT_INDEX
        var settings: Map<String, Any> = DEFAULT_SETTINGS

        fun index(index: String): Builder {
            this.index = index
            return this
        }

        fun settings(settings: Map<String, Any>): Builder {
            this.settings = settings
            return this
        }

        fun build(): SingleIndexMapping {
            return SingleIndexMapping(index, settings)
        }
    }
}
