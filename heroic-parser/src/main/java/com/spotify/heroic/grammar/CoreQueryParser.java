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
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class CoreQueryParser implements QueryParser {
    @Inject
    public CoreQueryParser() {
    }

    @Override
    public List<Expression> parse(String statements) {
        return parse(HeroicQueryParser::statements, statements)
            .pop(QueryListener.Statements.class)
            .getExpressions();
    }

    @Override
    public Filter parseFilter(String filter) {
        return parse(HeroicQueryParser::filterOnly, filter).pop(Filter.class).optimize();
    }

    FromDSL parseFrom(final String input) {
        final QueryListener listener = parse(HeroicQueryParser::from, input);
        listener.popMark(QueryListener.QueryMark.FROM);
        final MetricType source = listener.pop(MetricType.class);
        final Optional<RangeExpression> range = listener.popOptional(RangeExpression.class);
        return new FromDSL(source, range);
    }

    QueryExpression parseQuery(final String input) {
        return parse(HeroicQueryParser::query, input).pop(QueryExpression.class);
    }

    Expression parseExpression(final String input) {
        return parse(HeroicQueryParser::expressionOnly, input).pop(Expression.class);
    }

    private QueryListener parse(Function<HeroicQueryParser, ParserRuleContext> op, String input) {
        final HeroicQueryLexer lexer = new HeroicQueryLexer(new ANTLRInputStream(input));

        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final HeroicQueryParser parser = new HeroicQueryParser(tokens);

        parser.removeErrorListeners();
        parser.setErrorHandler(new BailErrorStrategy());

        final ParserRuleContext context;

        try {
            context = op.apply(parser);
        } catch (final ParseCancellationException e) {
            if (!(e.getCause() instanceof RecognitionException)) {
                throw e;
            }

            throw toParseException((RecognitionException) e.getCause());
        }

        final QueryListener listener = new QueryListener();

        ParseTreeWalker.DEFAULT.walk(listener, context);

        final Token last = lexer.getToken();

        if (last.getType() != Token.EOF) {
            throw new ParseException(
                String.format("garbage at end of string: '%s'", last.getText()), null,
                last.getLine(), last.getCharPositionInLine());
        }

        return listener;
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
