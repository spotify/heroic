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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.TimeUnit;

/**
 * int's are represented internally as longs.
 *
 * @author udoprog
 */
@Data
@EqualsAndHashCode(exclude = {"ctx"})
@JsonTypeName("integer")
@RequiredArgsConstructor
public final class IntegerExpression implements Expression {
    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final long value;

    @JsonCreator
    public IntegerExpression(
        @JsonProperty("value") final long value
    ) {
        this(Context.empty(), value);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitInteger(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    @Override
    public IntegerExpression multiply(Expression other) {
        return new IntegerExpression(ctx.join(other.context()),
            value * other.cast(IntegerExpression.class).value);
    }

    @Override
    public IntegerExpression divide(Expression other) {
        return new IntegerExpression(ctx.join(other.context()),
            value / other.cast(IntegerExpression.class).value);
    }

    @Override
    public IntegerExpression sub(Expression other) {
        return new IntegerExpression(ctx.join(other.context()),
            value - other.cast(IntegerExpression.class).value);
    }

    @Override
    public IntegerExpression add(Expression other) {
        return new IntegerExpression(ctx.join(other.context()),
            value + other.cast(IntegerExpression.class).value);
    }

    @Override
    public IntegerExpression negate() {
        return new IntegerExpression(ctx, -value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Expression> T cast(Class<T> to) {
        if (to.isAssignableFrom(IntegerExpression.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(DurationExpression.class)) {
            return (T) new DurationExpression(ctx, TimeUnit.MILLISECONDS, value);
        }

        if (to.isAssignableFrom(DoubleExpression.class)) {
            return (T) new DoubleExpression(ctx, ((Long) value).doubleValue());
        }

        throw ctx.castError(this, to);
    }

    public String toString() {
        return String.format("<%d>", value);
    }
}
