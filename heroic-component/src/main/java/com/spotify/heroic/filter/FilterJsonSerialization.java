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

package com.spotify.heroic.filter;

import java.io.IOException;

public interface FilterJsonSerialization<T> {
    public interface Deserializer {
        /**
         * read next item as a string.
         */
        String string() throws IOException;

        /**
         * read next item as a filter.
         */
        Filter filter() throws IOException;
    }

    public interface Serializer {
        /**
         * Serialize next item as a string.
         */
        void string(String string) throws IOException;

        /**
         * Serialize next item as a filter.
         */
        void filter(Filter filter) throws IOException;
    }

    public T deserialize(Deserializer deserializer) throws IOException;

    public void serialize(Serializer serializer, T filter) throws IOException;
}
