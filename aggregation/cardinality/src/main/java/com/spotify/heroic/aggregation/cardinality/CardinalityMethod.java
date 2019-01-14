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

package com.spotify.heroic.aggregation.cardinality;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.grammar.DoubleExpression;
import com.spotify.heroic.grammar.Expression;
import com.spotify.heroic.grammar.FunctionExpression;
import com.spotify.heroic.grammar.IntegerExpression;
import com.spotify.heroic.grammar.StringExpression;
import java.beans.ConstructorProperties;
import java.util.Optional;
import lombok.Data;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(CardinalityMethod.HyperLogLogCardinalityMethod.class),
    @JsonSubTypes.Type(CardinalityMethod.ExactCardinalityMethod.class)
})
public interface CardinalityMethod {
    CardinalityBucket build(final long timestamp);

    default CardinalityMethod reducer() {
        throw new RuntimeException("reducer not supported");
    }

    void hashTo(ObjectHasher hasher);

    @Data
    @JsonTypeName("exact")
    class ExactCardinalityMethod implements CardinalityMethod {
        public static final boolean DEFAULT_INCLUDE_KEY = false;

        private final boolean includeKey;

        @ConstructorProperties({"includeKey"})
        public ExactCardinalityMethod(final Optional<Boolean> includeKey) {
            this.includeKey = includeKey.orElse(DEFAULT_INCLUDE_KEY);
        }

        @Override
        public CardinalityBucket build(final long timestamp) {
            return new ExactCardinalityBucket(timestamp, includeKey);
        }

        @Override
        public void hashTo(final ObjectHasher hasher) {
            hasher.putObject(getClass(), () -> {
                hasher.putField("includeKey", includeKey, hasher.bool());
            });
        }
    }

    @Data
    @JsonTypeName("hll")
    class HyperLogLogCardinalityMethod implements CardinalityMethod {
        public static final double DEFAULT_PRECISION = 0.01;
        public static final boolean DEFAULT_INCLUDE_KEY = false;

        private final double precision;
        private final boolean includeKey;

        @ConstructorProperties({"precision", "includeKey"})
        public HyperLogLogCardinalityMethod(
            Optional<Double> precision, Optional<Boolean> includeKey
        ) {
            this.precision = precision.orElse(DEFAULT_PRECISION);
            this.includeKey = includeKey.orElse(DEFAULT_INCLUDE_KEY);
        }

        @Override
        public CardinalityBucket build(final long timestamp) {
            return new HyperLogLogCardinalityBucket(timestamp, includeKey, precision);
        }

        @Override
        public CardinalityMethod reducer() {
            return new ReduceHyperLogLogCardinalityMethod();
        }

        @Override
        public void hashTo(final ObjectHasher hasher) {
            hasher.putObject(this.getClass(), () -> {
                hasher.putField("precision", precision, hasher.doubleValue());
                hasher.putField("includeKey", includeKey, hasher.bool());
            });
        }
    }

    @Data
    @JsonTypeName("hllp")
    class HyperLogLogPlusCardinalityMethod implements CardinalityMethod {
        public static final int DEFAULT_PRECISION = 16;
        public static final boolean DEFAULT_INCLUDE_KEY = false;

        private final int precision;
        private final boolean includeKey;

        @ConstructorProperties({"precision", "includeKey"})
        public HyperLogLogPlusCardinalityMethod(
            Optional<Integer> precision, Optional<Boolean> includeKey
        ) {
            this.precision = precision.orElse(DEFAULT_PRECISION);
            this.includeKey = includeKey.orElse(DEFAULT_INCLUDE_KEY);
        }

        @Override
        public CardinalityBucket build(final long timestamp) {
            return new HyperLogLogPlusCardinalityBucket(timestamp, includeKey, precision);
        }

        @Override
        public CardinalityMethod reducer() {
            return new ReduceHyperLogLogPlusCardinalityMethod();
        }

        @Override
        public void hashTo(final ObjectHasher hasher) {
            hasher.putObject(this.getClass(), () -> {
                hasher.putField("precision", precision, hasher.integer());
                hasher.putField("includeKey", includeKey, hasher.bool());
            });
        }
    }

    @Data
    class ReduceHyperLogLogCardinalityMethod implements CardinalityMethod {
        @Override
        public CardinalityBucket build(final long timestamp) {
            return new ReduceHyperLogLogCardinalityBucket(timestamp);
        }

        @Override
        public void hashTo(final ObjectHasher hasher) {
            hasher.putObject(getClass());
        }
    }

    @Data
    class ReduceHyperLogLogPlusCardinalityMethod implements CardinalityMethod {
        @Override
        public CardinalityBucket build(final long timestamp) {
            return new ReduceHyperLogLogPlusCardinalityBucket(timestamp);
        }

        @Override
        public void hashTo(final ObjectHasher hasher) {
            hasher.putObject(getClass());
        }
    }

    static CardinalityMethod fromExpression(final Expression expression) {
        return expression.visit(new Expression.Visitor<CardinalityMethod>() {
            @Override
            public CardinalityMethod visitString(final StringExpression e) {
                return visitFunction(e.cast(FunctionExpression.class));
            }

            @Override
            public CardinalityMethod visitFunction(final FunctionExpression e) {
                switch (e.getName()) {
                    case "exact":
                        return buildExact(e);
                    case "hll":
                        return buildHyperLogLog(e);
                    case "hllp":
                        return buildHyperLogLogPlus(e);
                    default:
                        throw e.getContext().error("Unknown cardinality method");
                }
            }

            @Override
            public CardinalityMethod defaultAction(final Expression e) {
                throw e.getContext().error("Unsupported method");
            }

            private CardinalityMethod buildExact(final FunctionExpression e) {
                final Optional<Boolean> includeKey = e
                    .keyword("includeKey")
                    .map(i -> "true".equals(i.cast(StringExpression.class).getString()));

                return new ExactCardinalityMethod(includeKey);
            }

            private CardinalityMethod buildHyperLogLog(final FunctionExpression e) {
                final Optional<Double> precision =
                    e.keyword("precision").map(i -> i.cast(DoubleExpression.class).getValue());

                final Optional<Boolean> includeKey = e
                    .keyword("includeKey")
                    .map(i -> "true".equals(i.cast(StringExpression.class).getString()));

                return new HyperLogLogCardinalityMethod(precision, includeKey);
            }

            private CardinalityMethod buildHyperLogLogPlus(final FunctionExpression e) {
                final Optional<Integer> precision = e
                    .keyword("precision")
                    .map(i -> i.cast(IntegerExpression.class).getValueAsInteger());

                final Optional<Boolean> includeKey = e
                    .keyword("includeKey")
                    .map(i -> "true".equals(i.cast(StringExpression.class).getString()));

                return new HyperLogLogPlusCardinalityMethod(precision, includeKey);
            }
        });
    }
}
