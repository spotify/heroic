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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
@RequiredArgsConstructor
public class Visitors {
    public static final String GROUP = "group";
    public static final String CHAIN = "chain";

    static final StatementsVisitor STATEMENTS = new StatementsVisitor();
    static final ExpressionVisitor EXPRESSION = new ExpressionVisitor();
    static final FilterVisitor FILTER = new FilterVisitor();
    static final FromVisitor FROM = new FromVisitor();
    static final KeywordValuesVisitor KEYWORD_VALUES = new KeywordValuesVisitor();
    static final KeywordValueVisitor KEYWORD_VALUE = new KeywordValueVisitor();

    abstract static class BaseVisitor<T> extends HeroicQueryBaseVisitor<T> {
        @Override
        public T visitChildren(final RuleNode node) {
            final T result = super.visitChildren(node);

            if (result == null) {
                throw new IllegalStateException("Failed to parse node: " + node);
            }

            return result;
        }
    }

    static class StatementsVisitor extends BaseVisitor<Statements> {
        @Override
        public Statements visitStatements(
            final HeroicQueryParser.StatementsContext ctx
        ) {
            final StatementVisitor statement = new StatementVisitor();
            List<Expression> expressions =
                ctx.statement().stream().map(s -> s.accept(statement)).collect(Collectors.toList());
            return new Statements(expressions);
        }
    }

    static class StatementVisitor extends BaseVisitor<Expression> {
        @Override
        public Expression visitLetStatement(
            final HeroicQueryParser.LetStatementContext ctx
        ) {
            final Context c = context(ctx);
            final ReferenceExpression reference =
                ctx.expr().accept(EXPRESSION).cast(ReferenceExpression.class);
            final Expression value = ctx.query().accept(EXPRESSION);
            return new LetExpression(c, reference, value);
        }

        @Override
        public Expression visitQueryStatement(
            final HeroicQueryParser.QueryStatementContext ctx
        ) {
            return ctx.query().accept(EXPRESSION).cast(QueryExpression.class);
        }
    }

    static class ExpressionVisitor extends BaseVisitor<Expression> {
        private static final Pattern DURATION_PATTERN = Pattern.compile("^([0-9]+)(.+)$");

        @Override
        public Expression visitExpressionPrecedence(
            final HeroicQueryParser.ExpressionPrecedenceContext ctx
        ) {
            return ctx.expr().accept(this);
        }

        @Override
        public Expression visitExpressionReference(
            final HeroicQueryParser.ExpressionReferenceContext ctx
        ) {
            final Context c = context(ctx);
            final String name = ctx.getChild(0).getText().substring(1);
            return new ReferenceExpression(c, name);
        }

        @Override
        public Expression visitSourceRangeAbsolute(
            HeroicQueryParser.SourceRangeAbsoluteContext ctx
        ) {
            final Context c = context(ctx);
            final Expression start = ctx.start.accept(this);
            final Expression end = ctx.end.accept(this);
            return new RangeExpression(c, start, end);
        }

        @Override
        public Expression visitSourceRangeRelative(
            HeroicQueryParser.SourceRangeRelativeContext ctx
        ) {
            final Context c = context(ctx);
            final Expression distance = ctx.distance.accept(this);
            final ReferenceExpression now = new ReferenceExpression(c, "now");
            final Expression start = new MinusExpression(c, now, distance);
            return new RangeExpression(c, start, now);
        }

        @Override
        public Expression visitExpressionDateTime(
            final HeroicQueryParser.ExpressionDateTimeContext ctx
        ) {
            final Context c = context(ctx);
            return DateTimeExpression.parse(c, ctx.getChild(1).getText());
        }

        @Override
        public Expression visitExpressionTime(
            final HeroicQueryParser.ExpressionTimeContext ctx
        ) {
            final Context c = context(ctx);
            return TimeExpression.parse(c, ctx.getChild(1).getText());
        }

        @Override
        public Expression visitExpressionNegate(
            final HeroicQueryParser.ExpressionNegateContext ctx
        ) {
            final Context c = context(ctx);
            final Expression expression = ctx.expr().accept(this);
            return new NegateExpression(c, expression);
        }

        @Override
        public Expression visitExpressionPlusMinus(
            final HeroicQueryParser.ExpressionPlusMinusContext ctx
        ) {
            final Context c = context(ctx);
            final Expression left = ctx.left.accept(this);
            final Expression right = ctx.right.accept(this);
            final String operator = ctx.operator.getText();

            switch (operator) {
                case "+":
                    return new PlusExpression(c, left, right);
                case "-":
                    return new MinusExpression(c, left, right);
                default:
                    throw c.error("Unsupported operator: " + operator);
            }
        }

        @Override
        public Expression visitExpressionDivMul(
            final HeroicQueryParser.ExpressionDivMulContext ctx
        ) {
            final Context c = context(ctx);
            final Expression left = ctx.left.accept(this);
            final Expression right = ctx.right.accept(this);
            final String operator = ctx.operator.getText();

            switch (operator) {
                case "/":
                    return new DivideExpression(c, left, right);
                case "*":
                    return new MultiplyExpression(c, left, right);
                default:
                    throw c.error("Unsupported operator: " + operator);
            }
        }

        @Override
        public Expression visitExpressionList(final HeroicQueryParser.ExpressionListContext ctx) {
            final Context c = context(ctx);
            final List<Expression> expressions =
                ctx.expr().stream().map(e -> e.accept(this)).collect(Collectors.toList());
            return new ListExpression(c, expressions);
        }

        @Override
        public Expression visitExpressionFunction(
            final HeroicQueryParser.ExpressionFunctionContext ctx
        ) {
            final Context c = context(ctx);

            final String name = ctx.getChild(0).getText();

            final List<Expression> arguments =
                ctx.expr().stream().map(e -> e.accept(this)).collect(Collectors.toList());

            final Map<String, Expression> keywords = ctx
                .keyValue()
                .stream()
                .map(k -> k.accept(KEYWORD_VALUE))
                .collect(Collectors.toMap(KeywordValue::getKey, KeywordValue::getExpression));

            return new FunctionExpression(c, name, arguments, keywords);
        }

        @Override
        public Expression visitExpressionInteger(HeroicQueryParser.ExpressionIntegerContext ctx) {
            return new IntegerExpression(context(ctx), Long.parseLong(ctx.getText()));
        }

        @Override
        public Expression visitExpressionFloat(HeroicQueryParser.ExpressionFloatContext ctx) {
            return new DoubleExpression(context(ctx), Double.parseDouble(ctx.getText()));
        }

        @Override
        public Expression visitString(HeroicQueryParser.StringContext ctx) {
            final ParseTree child = ctx.getChild(0);
            final CommonToken token = (CommonToken) child.getPayload();
            final Context c = context(ctx);

            if (token.getType() == HeroicQueryLexer.SimpleString ||
                token.getType() == HeroicQueryLexer.Identifier) {
                return new StringExpression(c, child.getText());
            }

            return new StringExpression(c, parseQuotedString(c, child.getText()));
        }

        @Override
        public Expression visitExpressionDuration(HeroicQueryParser.ExpressionDurationContext ctx) {
            final Context c = context(ctx);

            final String text = ctx.getText();

            final Matcher m = DURATION_PATTERN.matcher(text);

            if (!m.matches()) {
                throw c.error("Invalid duration (" + text + ")");
            }

            final int value = Integer.parseInt(m.group(1));
            final TimeUnit unit = extractUnit(c, m.group(2));

            return new DurationExpression(c, unit, value);
        }

        @Override
        public Expression visitQuery(final HeroicQueryParser.QueryContext ctx) {
            final Context c = context(ctx);

            final Expression aggregation = ctx.select.accept(EXPRESSION);
            final Optional<From> from = Optional.ofNullable(ctx.from()).map(v -> v.accept(FROM));
            final Optional<MetricType> source = from.map(From::getType);
            final Optional<RangeExpression> range = from.flatMap(From::getRange);
            final Optional<Filter> where =
                Optional.ofNullable(ctx.where()).map(v -> v.accept(FILTER));
            final Optional<Map<String, Expression>> with =
                Optional.ofNullable(ctx.with()).map(v -> v.accept(KEYWORD_VALUES));
            final Optional<Map<String, Expression>> as =
                Optional.ofNullable(ctx.as()).map(v -> v.accept(KEYWORD_VALUES));

            return new QueryExpression(c, aggregation, source, range, where,
                with.orElseGet(ImmutableMap::of), as.orElseGet(ImmutableMap::of));
        }

        @Override
        public Expression visitExpressionBy(final HeroicQueryParser.ExpressionByContext ctx) {
            final Context c = context(ctx);

            final FunctionExpression left =
                ctx.expression.accept(this).cast(FunctionExpression.class);

            final Expression group = ctx.grouping.accept(this);

            return new FunctionExpression(c, GROUP, ImmutableList.of(group, left),
                ImmutableMap.of());
        }

        @Override
        public Expression visitExpressionPipe(final HeroicQueryParser.ExpressionPipeContext ctx) {
            final Context c = context(ctx);

            final List<Expression> values =
                ctx.expr().stream().map(e -> e.accept(this)).collect(Collectors.toList());

            return new FunctionExpression(c, CHAIN, values, ImmutableMap.of());
        }

        @Override
        public Expression visitExpressionAny(final HeroicQueryParser.ExpressionAnyContext ctx) {
            return new EmptyExpression(context(ctx));
        }

        private TimeUnit extractUnit(Context ctx, String text) {
            if ("ms".equals(text)) {
                return TimeUnit.MILLISECONDS;
            }

            if ("s".equals(text)) {
                return TimeUnit.SECONDS;
            }

            if ("m".equals(text)) {
                return TimeUnit.MINUTES;
            }

            if ("H".equals(text) || "h".equals(text)) {
                return TimeUnit.HOURS;
            }

            if ("d".equals(text)) {
                return TimeUnit.DAYS;
            }

            throw ctx.error("illegal unit: " + text);
        }

        private String parseQuotedString(Context ctx, String text) {
            int i = 0;
            boolean escapeNext = false;
            int unicodeEscape = 0;
            char unicodeChar = 0;

            final StringBuilder builder = new StringBuilder();

            while (i < text.length()) {
                final char c = text.charAt(i++);

                // skip first and last
                if (i == 1 || i == text.length()) {
                    continue;
                }

                if (unicodeEscape > 0) {
                    unicodeEscape--;
                    unicodeChar += (hexDigit(c) << (unicodeEscape * 4));

                    if (unicodeEscape == 0) {
                        builder.append(unicodeChar);
                    }

                    continue;
                }

                if (escapeNext) {
                    if (c == 'b') {
                        builder.append("\b");
                    } else if (c == 't') {
                        builder.append("\t");
                    } else if (c == 'n') {
                        builder.append("\n");
                    } else if (c == 'f') {
                        builder.append("\f");
                    } else if (c == 'r') {
                        builder.append("\r");
                    } else if (c == 'u') {
                        unicodeEscape = 4;
                    } else {
                        builder.append(c);
                    }

                    escapeNext = false;
                    continue;
                }

                if (c == '\\') {
                    escapeNext = true;
                    continue;
                }

                builder.append(c);
            }

            if (escapeNext) {
                throw ctx.error("expected escape sequence");
            }

            if (unicodeEscape > 0) {
                throw ctx.error("open unicode escape sequence");
            }

            return builder.toString();
        }

        private int hexDigit(final char c) {
            if (c >= 'a' && c <= 'f') {
                return c - 'a' + 10;
            }

            if (c >= '0' && c <= '9') {
                return c - '0';
            }

            throw new IllegalArgumentException("bad character: " + c);
        }
    }

    static class KeywordValueVisitor extends BaseVisitor<KeywordValue> {
        @Override
        public KeywordValue visitKeyValue(
            final HeroicQueryParser.KeyValueContext ctx
        ) {
            final Expression expression = ctx.expr().accept(EXPRESSION);
            return new KeywordValue(ctx.getChild(0).getText(), expression);
        }
    }

    static class KeywordValuesVisitor extends BaseVisitor<Map<String, Expression>> {
        @Override
        public Map<String, Expression> visitKeyValues(
            final HeroicQueryParser.KeyValuesContext ctx
        ) {
            return ctx
                .keyValue()
                .stream()
                .map(v -> v.accept(KEYWORD_VALUE))
                .collect(Collectors.toMap(KeywordValue::getKey, KeywordValue::getExpression));
        }
    }

    static class FromVisitor extends BaseVisitor<From> {
        @Override
        public From visitFrom(final HeroicQueryParser.FromContext ctx) {
            final String sourceName = ctx.Identifier().getText();
            final MetricType type = MetricType
                .fromIdentifier(sourceName)
                .orElseThrow(() -> new IllegalArgumentException("Illegal source: " + sourceName));
            final Optional<RangeExpression> range = Optional
                .ofNullable(ctx.sourceRange())
                .map(r -> r.accept(EXPRESSION).cast(RangeExpression.class));
            return new From(type, range);
        }
    }

    @RequiredArgsConstructor
    static class FilterVisitor extends BaseVisitor<Filter> {
        @Override
        public Filter visitFilterPrecedence(final HeroicQueryParser.FilterPrecedenceContext ctx) {
            return ctx.filter().accept(this);
        }

        @Override
        public Filter visitFilterBoolean(final HeroicQueryParser.FilterBooleanContext ctx) {
            final Context c = context(ctx);
            final String literal = ctx.getText();

            if ("true".equals(literal)) {
                return TrueFilter.get();
            }

            if ("false".equals(literal)) {
                return FalseFilter.get();
            }

            throw c.error("Invalid boolean literal: " + literal);
        }

        @Override
        public Filter visitFilterIn(HeroicQueryParser.FilterInContext ctx) {
            final Context c = context(ctx);
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final Expression match = ctx.value.accept(EXPRESSION);

            return new OrFilter(buildIn(c, key, match));
        }

        @Override
        public Filter visitFilterNotIn(HeroicQueryParser.FilterNotInContext ctx) {
            final Context c = context(ctx);
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final Expression match = ctx.value.accept(EXPRESSION);

            return new NotFilter(new OrFilter(buildIn(c, key, match)));
        }

        @Override
        public Filter visitFilterHas(HeroicQueryParser.FilterHasContext ctx) {
            final StringExpression value =
                ctx.expr().accept(EXPRESSION).cast(StringExpression.class);

            return new HasTagFilter(value.getString());
        }

        @Override
        public Filter visitFilterNot(HeroicQueryParser.FilterNotContext ctx) {
            final Filter filter = ctx.filter().accept(this);
            return new NotFilter(filter);
        }

        @Override
        public Filter visitFilterKeyEq(HeroicQueryParser.FilterKeyEqContext ctx) {
            final StringExpression value =
                ctx.expr().accept(EXPRESSION).cast(StringExpression.class);

            return new MatchKeyFilter(value.getString());
        }

        @Override
        public Filter visitFilterKeyNotEq(HeroicQueryParser.FilterKeyNotEqContext ctx) {
            final StringExpression value =
                ctx.expr().accept(EXPRESSION).cast(StringExpression.class);

            return new NotFilter(new MatchKeyFilter(value.getString()));
        }

        @Override
        public Filter visitFilterEq(HeroicQueryParser.FilterEqContext ctx) {
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final StringExpression value =
                ctx.value.accept(EXPRESSION).cast(StringExpression.class);

            return new MatchTagFilter(key.getString(), value.getString());
        }

        @Override
        public Filter visitFilterNotEq(HeroicQueryParser.FilterNotEqContext ctx) {
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final StringExpression value =
                ctx.value.accept(EXPRESSION).cast(StringExpression.class);

            return new NotFilter(new MatchTagFilter(key.getString(), value.getString()));
        }

        @Override
        public Filter visitFilterPrefix(HeroicQueryParser.FilterPrefixContext ctx) {
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final StringExpression value =
                ctx.value.accept(EXPRESSION).cast(StringExpression.class);

            return new StartsWithFilter(key.getString(), value.getString());
        }

        @Override
        public Filter visitFilterNotPrefix(HeroicQueryParser.FilterNotPrefixContext ctx) {
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final StringExpression value =
                ctx.value.accept(EXPRESSION).cast(StringExpression.class);

            return new NotFilter(new StartsWithFilter(key.getString(), value.getString()));
        }

        @Override
        public Filter visitFilterRegex(HeroicQueryParser.FilterRegexContext ctx) {
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final StringExpression value =
                ctx.value.accept(EXPRESSION).cast(StringExpression.class);

            return new RegexFilter(key.getString(), value.getString());
        }

        @Override
        public Filter visitFilterNotRegex(HeroicQueryParser.FilterNotRegexContext ctx) {
            final StringExpression key = ctx.key.accept(EXPRESSION).cast(StringExpression.class);
            final StringExpression value =
                ctx.value.accept(EXPRESSION).cast(StringExpression.class);

            return new NotFilter(new RegexFilter(key.getString(), value.getString()));
        }

        @Override
        public Filter visitFilterAnd(HeroicQueryParser.FilterAndContext ctx) {
            final Filter left = ctx.left.accept(this);
            final Filter right = ctx.right.accept(this);
            return new AndFilter(ImmutableList.of(left, right));
        }

        @Override
        public Filter visitFilterOr(HeroicQueryParser.FilterOrContext ctx) {
            final Filter left = ctx.left.accept(this);
            final Filter right = ctx.right.accept(this);
            return new OrFilter(ImmutableList.of(left, right));
        }

        private List<Filter> buildIn(
            final Context c, final StringExpression key, final Expression match
        ) {
            if (match instanceof StringExpression) {
                return ImmutableList.of(new MatchTagFilter(key.getString(),
                    match.cast(StringExpression.class).getString()));
            }

            if (!(match instanceof ListExpression)) {
                throw c.error("Cannot use type " + match + " in expression");
            }

            final List<Filter> values = new ArrayList<>();

            for (final Expression v : ((ListExpression) match).getList()) {
                values.add(new MatchTagFilter(key.getString(),
                    v.cast(StringExpression.class).getString()));
            }

            return values;
        }
    }

    @Data
    public static class From {
        private final MetricType type;
        private final Optional<RangeExpression> range;
    }

    private static Context context(final ParserRuleContext source) {
        int line = source.getStart().getLine() - 1;
        int col = source.getStart().getStartIndex();
        int lineEnd = source.getStop().getLine() - 1;
        int colEnd = source.getStop().getStopIndex();
        return new Context(line, col, lineEnd, colEnd);
    }
}
