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
@JsonTypeName("double")
@RequiredArgsConstructor
public final class DoubleExpression implements Expression {
    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final double value;

    @JsonCreator
    public DoubleExpression(
        @JsonProperty("value") final double value
    ) {
        this(Context.empty(), value);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitDouble(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    @Override
    public Expression multiply(Expression other) {
        return new DoubleExpression(ctx.join(other.context()),
            value * other.cast(DoubleExpression.class).value);
    }

    @Override
    public Expression divide(Expression other) {
        return new DoubleExpression(ctx.join(other.context()),
            value / other.cast(DoubleExpression.class).value);
    }

    @Override
    public Expression sub(Expression other) {
        return new DoubleExpression(ctx.join(other.context()),
            value - other.cast(DoubleExpression.class).value);
    }

    @Override
    public Expression add(Expression other) {
        return new DoubleExpression(ctx.join(other.context()),
            value + other.cast(DoubleExpression.class).value);
    }

    @Override
    public Expression negate() {
        return new DoubleExpression(ctx, -value);
    }

    public String toString() {
        return String.format("<%f>", value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Expression> T cast(Class<T> to) {
        if (to.isAssignableFrom(DoubleExpression.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(Long.class)) {
            return (T) new IntegerExpression(((Double) value).longValue());
        }

        if (to.isAssignableFrom(DurationExpression.class)) {
            return (T) new DurationExpression(ctx, TimeUnit.MILLISECONDS, (long) value);
        }

        throw ctx.castError(this, to);
    }
}
