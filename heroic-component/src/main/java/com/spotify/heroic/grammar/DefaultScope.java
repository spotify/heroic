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

package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class DefaultScope implements Expression.Scope {
    final Map<String, Expression> scope;

    public DefaultScope(final long now) {
        this.scope = buildScope(now);
    }

    private Map<String, Expression> buildScope(final long now) {
        final ImmutableMap.Builder<String, Expression> scope = ImmutableMap.builder();
        scope.put(Expression.NOW, Expression.integer(now));
        return scope.build();
    }

    @Override
    public Expression lookup(final Context c, final String name) {
        final Expression variable = scope.get(name);

        if (variable == null) {
            throw c.scopeLookupError(name);
        }

        return variable;
    }
}
