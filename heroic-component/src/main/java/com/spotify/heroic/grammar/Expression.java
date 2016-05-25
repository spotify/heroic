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
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;

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
        throw context().error(String.format("%s: unsupported unary operator: -", this));
    }

    default Expression multiply(Expression other) {
        throw context().error(String.format("%s: unsupported binary operator: *", this));
    }

    default Expression divide(Expression other) {
        throw context().error(String.format("%s: unsupported binary operator: /", this));
    }

    default Expression sub(Expression other) {
        throw context().error(String.format("%s: unsupported binary operator: -", this));
    }

    default Expression add(Expression other) {
        throw context().error(String.format("%s: unsupported binary operator: +", this));
    }

    default <T extends Expression> T cast(Class<T> to) {
        if (to.isAssignableFrom(getClass())) {
            return (T) this;
        }

        throw context().castError(this, to);
    }

    Context context();

    default Optional<Expression> toOptional() {
        return Optional.of(this);
    }

    static FunctionExpression function(String name) {
        return function(name, list(), ImmutableMap.of());
    }

    static FunctionExpression function(String name, ListExpression arguments) {
        return function(name, arguments, ImmutableMap.of());
    }

    static FunctionExpression function(
        String name, ListExpression arguments, Map<String, Expression> keywords
    ) {
        return new FunctionExpression(Context.empty(), name, arguments, keywords);
    }

    static ListExpression list(Expression... expressions) {
        return list(ImmutableList.copyOf(expressions));
    }

    static ListExpression list(List<Expression> expressions) {
        return new ListExpression(Context.empty(), ImmutableList.copyOf(expressions));
    }

    static DurationExpression duration(TimeUnit unit, long value) {
        return new DurationExpression(Context.empty(), unit, value);
    }

    static StringExpression string(String string) {
        return new StringExpression(Context.empty(), string);
    }

    static IntegerExpression number(long value) {
        return new IntegerExpression(Context.empty(), value);
    }

    static ReferenceExpression reference(final String name) {
        return new ReferenceExpression(Context.empty(), name);
    }

    static PlusExpression plus(final Expression left, final Expression right) {
        return new PlusExpression(Context.empty(), left, right);
    }

    static MinusExpression minus(final Expression left, final Expression right) {
        return new MinusExpression(Context.empty(), left, right);
    }

    static LetExpression let(final ReferenceExpression reference, final Expression expression) {
        return new LetExpression(Context.empty(), reference, expression);
    }

    static QueryExpression query(
        final Optional<Expression> select, final Optional<MetricType> source,
        final Optional<RangeExpression> range, final Optional<Filter> filter,
        final Map<String, Expression> with, final Map<String, Expression> as
    ) {
        return new QueryExpression(Context.empty(), select, source, range, filter, with, as);
    }

    static Expression empty() {
        return new EmptyExpression(Context.empty());
    }

    static Expression integer(final long value) {
        return new IntegerExpression(Context.empty(), value);
    }

    static RangeExpression range(final Expression start, final Expression end) {
        return new RangeExpression(Context.empty(), start, end);
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
        default Expression lookup(final String name) {
            return lookup(Context.empty(), name);
        }

        Expression lookup(final Context c, final String name);
    }
}
