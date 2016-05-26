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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Data;

/**
 * An expression representing a String.
 */
@Data
@JsonTypeName("string")
public final class StringExpression implements Expression {
    private final Context context;
    private final String string;

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitString(this);
    }

    @Override
    public StringExpression add(Expression other) {
        final StringExpression o = other.cast(StringExpression.class);
        return new StringExpression(context.join(o.context), string + o.string);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Expression> T cast(Class<T> to) {
        if (to.isAssignableFrom(StringExpression.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(ListExpression.class)) {
            return (T) Expression.list(context, this);
        }

        if (to.isAssignableFrom(FunctionExpression.class)) {
            return (T) new FunctionExpression(context, string, ImmutableList.of(),
                ImmutableMap.of());
        }

        throw context.castError(this, to);
    }

    @Override
    public String toRepr() {
        return QueryParser.escapeString(string);
    }
}
