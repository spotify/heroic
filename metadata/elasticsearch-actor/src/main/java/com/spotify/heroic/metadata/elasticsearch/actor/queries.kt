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

import com.spotify.heroic.common.OptionalLimit
import com.spotify.heroic.common.Series
import com.spotify.heroic.elasticsearch.Connection
import com.spotify.heroic.elasticsearch.LimitedSet
import com.spotify.heroic.filter.Filter
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import java.util.function.Consumer

const val TYPE_METADATA = "metadata"
const val KEY = "key"

private const val TAGS = "tags"
private const val TAG_KEYS = "tag_keys"
private const val TAG_DELIMITER = '\u0000'

private const val WRITE_CACHE_SIZE = "write-cache-size"
private val FILTER_CONVERTER = FilterConverter(KEY, TAGS, TAG_DELIMITER)

private val SCROLL_TIME = TimeValue.timeValueSeconds(5)
private const val SCROLL_SIZE = 1000


fun toSeries(hit: SearchHit): Series {
    val key = hit.source[KEY] as String
    @Suppress("UNCHECKED_CAST")
    val tags = (hit.source[TAGS] as List<String>).map(::buildTag).toMap()
    return Series.of(key, tags)
}


fun buildTag(kv: String): Pair<String, String> {
    val index = kv.indexOf(TAG_DELIMITER)
    if (index == -1) throw IllegalArgumentException("invalid tag source: $kv")

    return kv.substring(0, index) to kv.substring(index + 1)
}


fun buildContext(builder: XContentBuilder, series: Series): XContentBuilder {
    builder.field(KEY, series.key)

    builder.startArray(TAGS)
    series.tags.forEach { (key, value) ->
        builder.value(key + TAG_DELIMITER + value)
    }
    builder.endArray()

    builder.startArray(TAG_KEYS)
    series.tags.keys.forEach { builder.value(it) }
    builder.endArray()

    return builder
}


fun <T, O> entries(
    connection: Connection,
    filter: Filter,
    limit: OptionalLimit,
    converter: (SearchHit) -> T,
    collector:(LimitedSet<T>) -> O,
    modifier: Consumer<SearchRequestBuilder>
): O {
    val f = filter.visit(FILTER_CONVERTER)

    val builder = connection.search(TYPE_METADATA)
        .setScroll(SCROLL_TIME)
        .setSize(limit.asMaxInteger(SCROLL_SIZE))
        .setQuery(BoolQueryBuilder().must(f))

    modifier.accept(builder)
    val scroll = scrollEntries(connection, builder, limit, converter)
    return collector(scroll)
}


fun <T> scrollEntries(
    connection: Connection,
    request: SearchRequestBuilder,
    limit: OptionalLimit,
    converter: (SearchHit) -> T
): LimitedSet<T> {
    val scrollFactory = { scrollId: String ->
        connection.prepareSearchScroll(scrollId).setScroll(SCROLL_TIME).get()
    }

    return ScrollTransform(limit, converter, scrollFactory).transform(request.get())

}


fun filter(filter: Filter): QueryBuilder = filter.visit(FILTER_CONVERTER)