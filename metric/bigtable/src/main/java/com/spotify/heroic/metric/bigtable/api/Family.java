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

import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import lombok.Data;

import java.util.Iterator;

@Data
public class Family {
    final String name;
    final Iterable<com.google.bigtable.v2.Column> columns;

    /**
     * Get an iterable of the latest cells in a given column.
     *
     * @return An iterable over the latest cell in the available columns.
     */
    public Iterable<LatestCellValueColumn> latestCellValue() {
        return () -> {
            final Iterator<com.google.bigtable.v2.Column> iterator = columns.iterator();

            return new Iterator<LatestCellValueColumn>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public LatestCellValueColumn next() {
                    final com.google.bigtable.v2.Column next = iterator.next();

                    if (next.getCellsCount() < 1) {
                        throw new IllegalStateException("empty cell");
                    }

                    final ByteString qualifier = next.getQualifier();
                    final ByteString value = next.getCells(0).getValue();

                    return new LatestCellValueColumn(qualifier, value);
                }

                @Override
                public void remove() {
                    throw new IllegalStateException();
                }
            };
        };
    }

    @Data
    public static class LatestCellValueColumn {
        final ByteString qualifier;
        final ByteString value;
    }
}
