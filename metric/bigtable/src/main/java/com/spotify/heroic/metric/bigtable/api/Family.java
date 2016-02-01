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

import com.google.protobuf.ByteString;

import java.util.Iterator;

import lombok.Data;

@Data
public class Family {
    final String name;
    final Iterable<Column> columns;

    public static Iterable<Column> makeColumnIterable(final com.google.bigtable.v1.Family family) {
        return new Iterable<Column>() {
            @Override
            public Iterator<Column> iterator() {
                final Iterator<com.google.bigtable.v1.Column> iterator =
                        family.getColumnsList().iterator();

                return new Iterator<Column>() {

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Column next() {
                        final com.google.bigtable.v1.Column next = iterator.next();

                        final ByteString qualifier = next.getQualifier();
                        final ByteString value = next.getCells(0).getValue();

                        return new Column(qualifier, value);
                    }

                    @Override
                    public void remove() {
                        throw new IllegalStateException();
                    }
                };
            }
        };
    }
}
