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
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.AggregationArguments;
import lombok.Data;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Data
@JsonTypeName("function")
public class FunctionExpression implements Expression {
    private final Context context;
    private final String name;
    private final List<Expression> arguments;
    private final Map<String, Expression> keywords;

    @Override
    public Expression eval(final Scope scope) {
        final List<Expression> evaledArguments = Expression.evalList(arguments, scope);
        final Map<String, Expression> evaledKeywords = Expression.evalMap(keywords, scope);
        return new FunctionExpression(context, name, evaledArguments, evaledKeywords);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitFunction(this);
    }

    @Override
    public String toRepr() {
        final Joiner args = Joiner.on(", ");

        final Iterator<String> a = arguments.stream().map(Expression::toRepr).iterator();

        final Iterator<String> k = keywords
            .entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + e.getValue().toRepr())
            .iterator();

        return name + "(" + args.join(Iterators.concat(a, k)) + ")";
    }

    public AggregationArguments arguments() {
        return new AggregationArguments(getArguments(), getKeywords());
    }

    public Optional<Expression> keyword(final String key) {
        return Optional.ofNullable(keywords.get(key));
    }

    public FunctionExpression withKeywords(final Map<String, Expression> keywords) {
        return new FunctionExpression(context, name, arguments, keywords);
    }
}
