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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(DoubleExpression.class), @JsonSubTypes.Type(DurationExpression.class),
    @JsonSubTypes.Type(DateTimeExpression.class), @JsonSubTypes.Type(TimeExpression.class),
    @JsonSubTypes.Type(InstantExpression.class), @JsonSubTypes.Type(EmptyExpression.class),
    @JsonSubTypes.Type(FunctionExpression.class), @JsonSubTypes.Type(IntegerExpression.class),
    @JsonSubTypes.Type(LetExpression.class), @JsonSubTypes.Type(ListExpression.class),
    @JsonSubTypes.Type(MinusExpression.class), @JsonSubTypes.Type(PlusExpression.class),
    @JsonSubTypes.Type(MultiplyExpression.class), @JsonSubTypes.Type(DivideExpression.class),
    @JsonSubTypes.Type(QueryExpression.class), @JsonSubTypes.Type(RangeExpression.class),
    @JsonSubTypes.Type(ReferenceExpression.class), @JsonSubTypes.Type(StringExpression.class),
    @JsonSubTypes.Type(NegateExpression.class)
})
public interface Expression {
    String NOW = "now";

    /**
     * Visit the current type to perform some type of transformation.
     */
    <R> R visit(Visitor<R> visitor);

    /**
     * Statically evaluate the current expression using built-in mechanisms if possible.
     */
    default Expression eval(Scope scope) {
        return this;
    }

    default Expression negate() {
        throw getContext().error(String.format("%s: unsupported unary operator: -", this));
    }

    default Expression multiply(Expression other) {
        throw getContext().error(String.format("%s: unsupported binary operator: *", this));
    }

    default Expression divide(Expression other) {
        throw getContext().error(String.format("%s: unsupported binary operator: /", this));
    }

    default Expression sub(Expression other) {
        throw getContext().error(String.format("%s: unsupported binary operator: -", this));
    }

    default Expression add(Expression other) {
        throw getContext().error(String.format("%s: unsupported binary operator: +", this));
    }

    default <T extends Expression> T cast(Class<T> to) {
        if (to.isAssignableFrom(getClass())) {
            return (T) this;
        }

        throw getContext().castError(this, to);
    }

    String toRepr();

    Context getContext();

    default Optional<Expression> toOptional() {
        return Optional.of(this);
    }

    static IntegerExpression integer(Context ctx, long value) {
        return new IntegerExpression(ctx, value);
    }

    static LetExpression let(
        Context ctx, ReferenceExpression reference, Expression value
    ) {
        return new LetExpression(ctx, reference, value);
    }

    static ReferenceExpression reference(Context ctx, String name) {
        return new ReferenceExpression(ctx, name);
    }

    static EmptyExpression empty(Context ctx) {
        return new EmptyExpression(ctx);
    }

    static ListExpression list(Context ctx, Expression... expressions) {
        return new ListExpression(ctx, ImmutableList.copyOf(expressions));
    }

    static FunctionExpression function(Context ctx, String name, Expression... arguments) {
        return new FunctionExpression(ctx, name, ImmutableList.copyOf(arguments),
            ImmutableMap.of());
    }

    static DurationExpression duration(final Context ctx, final TimeUnit unit, final long value) {
        return new DurationExpression(ctx, unit, value);
    }

    static StringExpression string(Context ctx, String value) {
        return new StringExpression(ctx, value);
    }

    static RangeExpression range(Context ctx, Expression start, Expression end) {
        return new RangeExpression(ctx, start, end);
    }

    static Map<String, Expression> evalMap(Map<String, Expression> input, Scope scope) {
        final ImmutableMap.Builder<String, Expression> evaled = ImmutableMap.builder();

        for (final Map.Entry<String, Expression> e : input.entrySet()) {
            evaled.put(e.getKey(), e.getValue().eval(scope));
        }

        return evaled.build();
    }

    static List<Expression> evalList(List<Expression> list, Scope scope) {
        final ImmutableList.Builder<Expression> evaled = ImmutableList.builder();

        for (final Expression e : list) {
            evaled.add(e.eval(scope));
        }

        return evaled.build();
    }

    interface Visitor<R> {
        default R visitEmpty(final EmptyExpression e) {
            return defaultAction(e);
        }

        default R visitDouble(final DoubleExpression e) {
            return defaultAction(e);
        }

        default R visitInteger(final IntegerExpression e) {
            return defaultAction(e);
        }

        default R visitString(final StringExpression e) {
            return defaultAction(e);
        }

        default R visitDuration(final DurationExpression e) {
            return defaultAction(e);
        }

        default R visitFunction(final FunctionExpression e) {
            return defaultAction(e);
        }

        default R visitList(final ListExpression e) {
            return defaultAction(e);
        }

        default R visitMultiply(MultiplyExpression e) {
            return defaultAction(e);
        }

        default R visitDivide(DivideExpression e) {
            return defaultAction(e);
        }

        default R visitPlus(final PlusExpression e) {
            return defaultAction(e);
        }

        default R visitMinus(final MinusExpression e) {
            return defaultAction(e);
        }

        default R visitQuery(final QueryExpression e) {
            return defaultAction(e);
        }

        default R visitLet(final LetExpression e) {
            return defaultAction(e);
        }

        default R visitReference(final ReferenceExpression e) {
            return defaultAction(e);
        }

        default R visitRange(final RangeExpression e) {
            return defaultAction(e);
        }

        default R visitNegate(final NegateExpression e) {
            return defaultAction(e);
        }

        default R visitTime(final TimeExpression e) {
            return defaultAction(e);
        }

        default R visitDateTime(final DateTimeExpression e) {
            return defaultAction(e);
        }

        default R visitInstant(final InstantExpression e) {
            return defaultAction(e);
        }

        default R defaultAction(final Expression e) {
            throw new IllegalStateException("Action not implemented for: " + e);
        }
    }

    interface Scope {
        Expression lookup(final Context c, final String name);
    }
}
