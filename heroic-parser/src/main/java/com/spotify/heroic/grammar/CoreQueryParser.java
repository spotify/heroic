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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.metric.MetricType;

import lombok.Data;

public class CoreQueryParser implements QueryParser {
    public static final Operation<Value> VALUE_EXPR = new Operation<Value>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.valueExpr();
        }

        public Value convert(QueryListener listener) {
            return listener.pop(Value.class);
        };
    };

    public static final Operation<QueryDSL> QUERY = new Operation<QueryDSL>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.query();
        }

        @Override
        public QueryDSL convert(QueryListener listener) {
            return listener.pop(QueryDSL.class);
        };
    };

    public static final Operation<Filter> FILTER = new Operation<Filter>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.filter();
        }

        @Override
        public Filter convert(QueryListener listener) {
            return listener.pop(Filter.class);
        };
    };

    public static final Operation<AggregationValue> AGGREGATION = new Operation<AggregationValue>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.aggregation();
        }

        @Override
        public AggregationValue convert(QueryListener listener) {
            return listener.pop(AggregationValue.class);
        };
    };

    public static final Operation<Optional<Aggregation>> SELECT = new Operation<Optional<Aggregation>>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.select();
        }

        @Override
        public Optional<Aggregation> convert(QueryListener listener) {
            listener.popMark(QueryListener.SELECT_MARK);
            return listener.popOptional(Aggregation.class);
        };
    };

    public static final Operation<FromDSL> FROM = new Operation<FromDSL>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.from();
        }

        @Override
        public FromDSL convert(QueryListener listener) {
            listener.popMark(QueryListener.FROM_MARK);
            final MetricType source = listener.pop(MetricType.class);
            final Optional<QueryDateRange> range = listener.popOptional(QueryDateRange.class);
            return new FromDSL(source, range);
        };
    };

    private final FilterFactory filters;
    private final AggregationFactory aggregations;

    @Inject
    public CoreQueryParser(FilterFactory filters, AggregationFactory aggregations) {
        this.filters = filters;
        this.aggregations = aggregations;
    }

    @Override
    public QueryDSL parseQuery(String query) {
        return parse(QUERY, query);
    }

    @Override
    public String stringifyQuery(final QueryDSL q) {
        final Joiner joiner = Joiner.on(" ");
        final Joiner groups = Joiner.on(", ");

        final List<String> parts = new ArrayList<>();

        parts.add(q.getAggregation().map(Aggregation::toDSL).orElse("*"));
        parts.add(String.format("from %s", formatSource(q)));

        q.getWhere().ifPresent(filter -> {
            parts.add("where " + filter.toDSL());
        });

        q.getGroupBy().ifPresent(groupBy -> {
            parts.add("group by " + groups.join(groupBy.stream().map(QueryParser::escapeString).iterator()));
        });

        return joiner.join(parts);
    }

    @Override
    public Filter parseFilter(String filter) {
        return parse(FILTER, filter);
    }

    @Override
    public Optional<Aggregation> parseAggregation(String aggregation) {
        return parse(SELECT, aggregation);
    }

    public <T> T parse(Operation<T> op, String input) {
        return parse(op, input, System.currentTimeMillis());
    }

    public <T> T parse(Operation<T> op, String input, final long now) {
        final HeroicQueryLexer lexer = new HeroicQueryLexer(new ANTLRInputStream(input));

        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final HeroicQueryParser parser = new HeroicQueryParser(tokens);

        parser.removeErrorListeners();
        parser.setErrorHandler(new BailErrorStrategy());

        final ParserRuleContext context;

        try {
            context = op.context(parser);
        } catch (final ParseCancellationException e) {
            if (!(e.getCause() instanceof RecognitionException)) {
                throw e;
            }

            throw toParseException((RecognitionException) e.getCause());
        }

        final QueryListener listener = new QueryListener(filters, aggregations, now);

        ParseTreeWalker.DEFAULT.walk(listener, context);

        final Token last = lexer.getToken();

        if (last.getType() != Token.EOF) {
            throw new ParseException(String.format("garbage at end of string: %s", last.getText()), last.getLine(),
                    last.getCharPositionInLine());
        }

        return op.convert(listener);
    }

    private String formatSource(final QueryDSL source) {
        return source.getRange().map(range -> {
            return source.getSource().identifier() + range.toDSL();
        }).orElseGet(source.getSource()::identifier);
    }

    private ParseException toParseException(final RecognitionException e) {
        final Token token = e.getOffendingToken();

        if (token.getType() == HeroicQueryLexer.UnterminatedQutoedString) {
            return new ParseException(String.format("unterminated string: %s", token.getText()), token.getLine(),
                    token.getCharPositionInLine());
        }

        return new ParseException("unexpected token: " + token.getText(), token.getLine(),
                token.getCharPositionInLine());
    }

    public static interface Operation<T> {
        public ParserRuleContext context(HeroicQueryParser parser);

        public T convert(final QueryListener listener);
    }

    @Data
    public static class FromDSL {
        private final MetricType source;
        private final Optional<QueryDateRange> range;
    }
}
