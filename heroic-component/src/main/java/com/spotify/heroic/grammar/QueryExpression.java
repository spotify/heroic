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
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Optional;

@Data
@JsonTypeName("query")
public class QueryExpression implements Expression {
    private final Context context;
    private final Expression select;
    private final Optional<MetricType> source;
    private final Optional<RangeExpression> range;
    private final Optional<Filter> filter;
    private final Map<String, Expression> with;
    private final Map<String, Expression> as;

    @Override
    public Expression eval(final Scope scope) {
        final Map<String, Expression> with = Expression.evalMap(this.with, scope);
        final Map<String, Expression> as = Expression.evalMap(this.as, scope);

        return new QueryExpression(context, select.eval(scope), source,
            range.map(r -> r.eval(scope)), filter, with, as);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitQuery(this);
    }

    @Override
    public String toRepr() {
        return String.format("{select: %s source: %s, range: %s, filter: %s, with: %s, as: %s}",
            select, source, range, filter, with, as);
    }

    public static Builder newBuilder(final Context context, final Expression select) {
        return new Builder(context, select);
    }

    @RequiredArgsConstructor
    public static class Builder {
        private final Context context;
        private final Expression select;

        private Optional<MetricType> source = Optional.empty();
        private Optional<RangeExpression> range = Optional.empty();
        private Optional<Filter> filter = Optional.empty();
        private Map<String, Expression> with = ImmutableMap.of();
        private Map<String, Expression> as = ImmutableMap.of();

        public Builder source(final MetricType source) {
            this.source = Optional.of(source);
            return this;
        }

        public Builder range(final RangeExpression range) {
            this.range = Optional.of(range);
            return this;
        }

        public Builder filter(final Filter filter) {
            this.filter = Optional.of(filter);
            return this;
        }

        public Builder with(final Map<String, Expression> with) {
            this.with = with;
            return this;
        }

        public Builder as(final Map<String, Expression> as) {
            this.as = as;
            return this;
        }

        public QueryExpression build() {
            return new QueryExpression(context, select, source, range, filter, with, as);
        }
    }
}
