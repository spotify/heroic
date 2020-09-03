/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.metric.bigtable.api

import java.lang.IllegalArgumentException
import java.util.*
import java.util.regex.Pattern

data class Table(
    val clusterUri: String,
    val name: String,
    val columnFamilies: Map<String, ColumnFamily>
) {
    constructor(clusterUri: String, name: String): this(clusterUri, name, emptyMap())

    fun getColumnFamily(name: String): Optional<ColumnFamily> {
        return Optional.ofNullable(columnFamilies[name])
    }

    fun toURI(): String = toURI(clusterUri, name)

    fun withAddColumnFamily(columnFamily: ColumnFamily): Table {
        return Table(clusterUri, name, columnFamilies.plus(columnFamily.name to columnFamily))
    }

    companion object {
        private val TABLE_NAME_PATTERN: Pattern =
            Pattern.compile("^(.+)\\/tables\\/([_a-zA-Z0-9][-_.a-zA-Z0-9]*)$")
        private const val TABLE_NAME_FORMAT = "%s/tables/%s"

        @JvmStatic fun fromPb(table: com.google.bigtable.admin.v2.Table): Table {
            val matcher = TABLE_NAME_PATTERN.matcher(table.name)
            if (!matcher.matches()) {
                throw IllegalArgumentException("Illegal table URI: ${table.name}")
            }

            val cluster: String = matcher.group(1)
            val tableId: String = matcher.group(2)

            val columnFamilies = table.columnFamiliesMap
                .map {
                    val columnFamily = ColumnFamily(cluster, tableId, it.key)
                    columnFamily.name to columnFamily
                }.toMap()
            return Table(cluster, tableId, columnFamilies)
        }

        @JvmStatic fun toURI(clusterUri: String, name: String): String =
            String.format(TABLE_NAME_FORMAT, clusterUri, name)
    }
}
