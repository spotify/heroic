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

import com.fasterxml.jackson.annotation.JsonProperty
import com.spotify.heroic.common.Duration
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.search.builder.SearchSourceBuilder
import java.util.concurrent.TimeUnit

private val DEFAULT_INTERVAL = Duration.of(7, TimeUnit.DAYS)
private const val DEFAULT_MAX_READ_INDICES = 2
private const val DEFAULT_MAX_WRITE_INDICES = 1
private const val DEFAULT_PATTERN = "heroic-%s"

private val OPTIONS = IndicesOptions.fromOptions(
    true, true, false, false)

data class RotatingIndexMapping(
    @JsonProperty("interval") private val intervalDuration: Duration = DEFAULT_INTERVAL,
    private val maxReadIndices: Int = DEFAULT_MAX_READ_INDICES,
    private val maxWriteIndices: Int = DEFAULT_MAX_WRITE_INDICES,
    private val pattern: String = DEFAULT_PATTERN,
    override val settings: Map<String, Any> = emptyMap()
): IndexMapping {
    private val interval = intervalDuration.convert(TimeUnit.MILLISECONDS)
    override val template = pattern.format("*")

    private fun indices(maxIndices: Int, now: Long, type: String): Array<String> {
        val curr = now - (now % interval)
        val indexPattern = pattern.replace("%s", "$type-%s")

        return (0 until maxIndices)
            .map { curr - (interval * it) }
            .takeWhile { it >= 0 }
            .map { indexPattern.format(it) }
            .toTypedArray()
    }

    @Throws(NoIndexSelectedException::class)
    fun readIndices(now: Long, type: String): Array<String> {
        val indices = indices(maxReadIndices, now, type)
        if (indices.isEmpty()) {
            throw NoIndexSelectedException()
        }
        return indices
    }

    @Throws(NoIndexSelectedException::class)
    override fun readIndices(type: String): Array<String> {
        return readIndices(System.currentTimeMillis(), type)
    }

    fun writeIndices(now: Long, type: String): Array<String> {
        return indices(maxWriteIndices, now, type)
    }

    override fun writeIndices(type: String): Array<String> {
        return writeIndices(System.currentTimeMillis(), type)
    }

    @Throws(NoIndexSelectedException::class)
    override fun search(type: String): SearchRequest {
        return SearchRequest(*readIndices(type)).indicesOptions(OPTIONS)
    }

    @Throws(NoIndexSelectedException::class)
    override fun count(type: String): SearchRequest {
        return search(type).source(SearchSourceBuilder().size(0))
    }

    @Throws(NoIndexSelectedException::class)
    override fun delete(type: String, id: String): List<DeleteRequest> {
        return readIndices(type).map { DeleteRequest(it, type, id) }
    }

    class Builder {
        var interval: Duration = DEFAULT_INTERVAL
        var maxReadIndices: Int = DEFAULT_MAX_READ_INDICES
        var maxWriteIndices: Int = DEFAULT_MAX_WRITE_INDICES
        var pattern: String = DEFAULT_PATTERN
        var settings: Map<String, Any> = emptyMap()

        fun interval(interval: Duration): Builder {
            this.interval = interval
            return this
        }

        fun maxReadIndices(maxReadIndices: Int): Builder {
            this.maxReadIndices = maxReadIndices
            return this
        }

        fun maxWriteIndices(maxWriteIndices: Int): Builder {
            this.maxWriteIndices = maxWriteIndices
            return this
        }

        fun pattern(pattern: String): Builder {
            this.pattern = pattern
            return this
        }

        fun settings(settings: Map<String, Any>): Builder {
            this.settings = settings
            return this
        }

        fun build(): RotatingIndexMapping {
            return RotatingIndexMapping(
                interval,
                maxReadIndices,
                maxWriteIndices,
                pattern,
                settings
            )
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }
}