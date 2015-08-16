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

package com.spotify.heroic.metric.astyanax;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.MapSerializer;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.ext.marshal.SafeUTF8Type;
import com.spotify.heroic.ext.serializers.SafeStringSerializer;

class SeriesSerializer extends AbstractSerializer<Series> {
    private static final SafeStringSerializer keySerializer = SafeStringSerializer.get();
    private static final MapSerializer<String, String> tagsSerializer = new MapSerializer<String, String>(
            SafeUTF8Type.instance, SafeUTF8Type.instance);

    private static final SeriesSerializer instance = new SeriesSerializer();

    public static SeriesSerializer get() {
        return instance;
    }

    private static final Comparator<String> COMPARATOR = new Comparator<String>() {
        public int compare(String a, String b) {
            if (a == null || b == null) {
                if (a == null)
                    return -1;

                if (b == null)
                    return 1;

                return 0;
            }

            return a.compareTo(b);
        }
    };

    @Override
    public ByteBuffer toByteBuffer(Series obj) {
        final Composite composite = new Composite();
        final Map<String, String> tags = new TreeMap<String, String>(COMPARATOR);
        tags.putAll(obj.getTags());

        composite.addComponent(obj.getKey(), keySerializer);
        composite.addComponent(tags, tagsSerializer);

        return composite.serialize();
    }

    @Override
    public Series fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);

        final String key = composite.get(0, keySerializer);
        final Map<String, String> tags = composite.get(1, tagsSerializer);

        return Series.of(key, tags);
    }
}