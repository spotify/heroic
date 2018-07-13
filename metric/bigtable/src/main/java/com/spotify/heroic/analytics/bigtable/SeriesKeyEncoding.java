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

package com.spotify.heroic.analytics.bigtable;

import com.spotify.heroic.bigtable.com.google.api.client.util.Charsets;
import com.google.common.base.Splitter;
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import com.spotify.heroic.common.Series;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.time.LocalDate;
import java.util.List;

@Data
public class SeriesKeyEncoding {
    private final String category;

    private static final Splitter SPLITTER = Splitter.on('/').limit(3);

    public SeriesKey decode(ByteString key, Transform<String, Series> transform) throws Exception {
        final String string = key.toString(Charsets.UTF_8);
        final List<String> parts = SPLITTER.splitToList(string);

        if (parts.size() != 3) {
            throw new IllegalArgumentException("Not a valid key: " + string);
        }

        final String category = parts.get(0);

        if (!this.category.equals(category)) {
            throw new IllegalArgumentException(
                "Key is in the wrong category (expected " + this.category + "): " + string);
        }

        final LocalDate date = LocalDate.parse(parts.get(1));
        final Series series = transform.transform(parts.get(2));
        return new SeriesKey(date, series);
    }

    public ByteString encode(SeriesKey key, Transform<Series, String> transform) throws Exception {
        return ByteString.copyFrom(
            category + "/" + key.getDate().toString() + "/" + transform.transform(key.getSeries()),
            Charsets.UTF_8);
    }

    public ByteString rangeKey(final LocalDate date) {
        return ByteString.copyFrom(category + "/" + date.toString(), Charsets.UTF_8);
    }

    @Data
    public static class SeriesKey {
        private final LocalDate date;
        private final Series series;
    }
}
