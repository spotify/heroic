/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.heroic.metric.bigtable.api;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
@RequiredArgsConstructor
public class Table {
    final String clusterUri;
    final String name;
    final Map<String, ColumnFamily> columnFamilies;

    public Table(final String cluster, final String name) {
        this(cluster, name, ImmutableMap.of());
    }

    private static final Pattern TABLE_NAME_PATTERN =
        Pattern.compile("^(.+)\\/tables\\/([_a-zA-Z0-9][-_.a-zA-Z0-9]*)$");

    private static final String TABLE_NAME_FORMAT = "%s/tables/%s";

    public String getName() {
        return name;
    }

    public Optional<ColumnFamily> getColumnFamily(final String name) {
        return Optional.ofNullable(columnFamilies.get(name));
    }

    public static Table fromPb(com.google.bigtable.admin.v2.Table table) {
        final Matcher m = TABLE_NAME_PATTERN.matcher(table.getName());

        if (!m.matches()) {
            throw new IllegalArgumentException("Illegal table URI: " + table.getName());
        }

        final String cluster = m.group(1);
        final String tableId = m.group(2);

        final ImmutableMap.Builder<String, ColumnFamily> columnFamilies = ImmutableMap.builder();

        for (final Entry<String, com.google.bigtable.admin.v2.ColumnFamily> e : table
            .getColumnFamilies()
            .entrySet()) {
            final ColumnFamily columnFamily = new ColumnFamily(cluster, tableId, e.getKey());
            columnFamilies.put(columnFamily.getName(), columnFamily);
        }

        return new Table(cluster, tableId, columnFamilies.build());
    }

    public String toURI() {
        return String.format(TABLE_NAME_FORMAT, clusterUri, name);
    }

    public static String toURI(final String clusterUri, final String name) {
        return String.format(TABLE_NAME_FORMAT, clusterUri, name);
    }
}
