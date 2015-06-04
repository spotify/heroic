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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.google.inject.Inject;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;

public class CoreQueryParser implements QueryParser {
    public static final Operation<Value> VALUE_EXPR = new Operation<Value>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.valueExpr();
        }

        @Override
        public Class<Value> type() {
            return Value.class;
        }
    };

    public static final Operation<QueryDSL> QUERY = new Operation<QueryDSL>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.query();
        }

        @Override
        public Class<QueryDSL> type() {
            return QueryDSL.class;
        }
    };

    public static final Operation<Filter> FILTER = new Operation<Filter>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.filter();
        }

        @Override
        public Class<Filter> type() {
            return Filter.class;
        }
    };

    public static final Operation<Queries> QUERIES = new Operation<Queries>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.query();
        }

        @Override
        public Class<Queries> type() {
            return Queries.class;
        }
    };

    public static final Operation<FromDSL> FROM = new Operation<FromDSL>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.from();
        }

        @Override
        public Class<FromDSL> type() {
            return FromDSL.class;
        }
    };

    public static final Operation<SelectDSL> SELECT = new Operation<SelectDSL>() {
        @Override
        public ParserRuleContext context(HeroicQueryParser parser) {
            return parser.select();
        }

        @Override
        public Class<SelectDSL> type() {
            return SelectDSL.class;
        }
    };

    private final FilterFactory filters;

    @Inject
    public CoreQueryParser(FilterFactory filters) {
        this.filters = filters;
    }

    @Override
    public QueryDSL parseQuery(String query) {
        return parse(QUERY, query);
    }

    @Override
    public Filter parseFilter(String filter) {
        return parse(FILTER, filter);
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
            if (!(e.getCause() instanceof RecognitionException))
                throw e;

            final RecognitionException r = (RecognitionException) e.getCause();
            final Token token = r.getOffendingToken();

            if (token.getType() == HeroicQueryLexer.UnterminatedQutoedString) {
                throw new ParseException(String.format("unterminated string (%s)", token.getText()), token.getLine(),
                        token.getCharPositionInLine());
            }

            throw new ParseException(String.format("syntax error (%s): unexpected input", token.getText()),
                    token.getLine(), token.getCharPositionInLine());
        }

        final QueryListener listener = new QueryListener(filters, now);

        ParseTreeWalker.DEFAULT.walk(listener, context);

        return (T) listener.pop(op.type());
    }

    public static interface Operation<T> {
        public ParserRuleContext context(HeroicQueryParser parser);

        public Class<T> type();
    }
}
