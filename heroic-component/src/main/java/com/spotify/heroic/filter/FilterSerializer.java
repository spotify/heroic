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

import eu.toolchain.serializer.Serializer;

public interface FilterSerializer extends Serializer<Filter> {
    public <T extends Filter.OneArg<A>, A> void register(String id, Class<T> type, OneArgumentFilter<T, A> builder,
            Serializer<A> first);

    public <T extends Filter.TwoArgs<A, B>, A, B> void register(String id, Class<T> type,
            TwoArgumentsFilter<T, A, B> builder, Serializer<A> first, Serializer<B> second);

    public <T extends Filter.MultiArgs<A>, A> void register(String id, Class<T> type,
            MultiArgumentsFilter<T, A> builder, Serializer<A> term);

    public <T extends Filter.NoArg> void register(String id, Class<T> type, NoArgumentFilter<T> builder);
}
