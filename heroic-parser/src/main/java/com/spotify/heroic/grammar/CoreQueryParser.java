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

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import javax.inject.Inject;
import java.util.Optional;

public class CoreQueryParser implements QueryParser {
    static final Operation<Statements> STATEMENTS = new Operation<Statements>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.statements();
        }

        public Statements convert(QueryListener listener) {
            return listener.pop(Statements.class);
        }
    };

    static final Operation<Expression> EXPRESSION = new Operation<Expression>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.expressionOnly();
        }

        public Expression convert(QueryListener listener) {
            return listener.pop(Expression.class);
        }
    };

    static final Operation<QueryExpression> QUERY = new Operation<QueryExpression>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.query();
        }

        @Override
        public QueryExpression convert(QueryListener listener) {
            return listener.pop(QueryExpression.class);
        }
    };

    static final Operation<Filter> FILTER = new Operation<Filter>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.filterOnly();
        }

        @Override
        public Filter convert(QueryListener listener) {
            return listener.pop(Filter.class).optimize();
        }
    };

    static final Operation<FunctionExpression> AGGREGATION = new Operation<FunctionExpression>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.expressionOnly();
        }

        @Override
        public FunctionExpression convert(QueryListener listener) {
            return listener.pop(FunctionExpression.class);
        }
    };

    static final Operation<FromDSL> FROM = new Operation<FromDSL>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.from();
        }

        @Override
        public FromDSL convert(QueryListener listener) {
            listener.popMark(QueryListener.QueryMark.FROM);
            final MetricType source = listener.pop(MetricType.class);
            final Optional<RangeExpression> range = listener.popOptional(RangeExpression.class);
            return new FromDSL(source, range);
        }
    };

    private final FilterFactory filters;

    @Inject
    public CoreQueryParser(FilterFactory filters) {
        this.filters = filters;
    }

    @Override
    public Statements parse(String statements) {
        return parse(STATEMENTS, statements);
    }

    @Override
    public Filter parseFilter(String filter) {
        return parse(FILTER, filter);
    }

    public <T> T parse(Operation<T> op, String input) {
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

        final QueryListener listener = new QueryListener(filters);

        ParseTreeWalker.DEFAULT.walk(listener, context);

        final Token last = lexer.getToken();

        if (last.getType() != Token.EOF) {
            throw new ParseException(
                String.format("garbage at end of string: '%s'", last.getText()), null,
                last.getLine(), last.getCharPositionInLine());
        }

        return op.convert(listener);
    }

    private ParseException toParseException(final RecognitionException e) {
        final Token token = e.getOffendingToken();

        if (token.getType() == HeroicQueryLexer.UnterminatedQutoedString) {
            return new ParseException(String.format("unterminated string: %s", token.getText()),
                null, token.getLine(), token.getCharPositionInLine());
        }

        return new ParseException("unexpected token: " + token.getText(), null, token.getLine(),
            token.getCharPositionInLine());
    }

    public interface Operation<T> {
        ParserRuleContext context(HeroicQueryParser parser);

        T convert(final QueryListener listener);
    }

    @Data
    public static class FromDSL {
        private final MetricType source;
        private final Optional<RangeExpression> range;

        public FromDSL eval(final Expression.Scope scope) {
            return new FromDSL(source, range.map(r -> r.eval(scope)));
        }
    }
}
