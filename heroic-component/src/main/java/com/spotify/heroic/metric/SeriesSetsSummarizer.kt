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

package com.spotify.heroic.metric

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimap
import com.spotify.heroic.common.Histogram
import com.spotify.heroic.common.Series

data class SeriesSetsSummarizer @JvmOverloads constructor(
    val uniqueKeys: HashSet<String> = hashSetOf(),
    val tags: Multimap<String, String> = HashMultimap.create(),
    val resource: Multimap<String, String> = HashMultimap.create(),
    val seriesSize: Histogram.Builder = Histogram.Builder()
) {
    fun add(series: Set<Series>) {
        seriesSize.add(series.size.toLong())
        series.forEach {s ->
            uniqueKeys.add(s.key)
            s.tags.forEach { tags.put(it.key, it.value) }
            s.resource.forEach { resource.put(it.key, it.value) }
        }
    }

    fun end(): Summary {
        val tagsSize = Histogram.Builder()
        val resourceSize = Histogram.Builder()

        tags.asMap().forEach { tagsSize.add(it.value.size.toLong()) }
        resource.asMap().forEach { resourceSize.add(it.value.size.toLong()) }

        return Summary(uniqueKeys.size.toLong(), tagsSize.build(), resourceSize.build(),
            seriesSize.build())
    }

    data class Summary(
        val uniqueKeys: Long,
        val tagsSize: Histogram,
        val resourceSize: Histogram,
        val seriesSize: Histogram
    )
}
