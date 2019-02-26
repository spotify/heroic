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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationByAllContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationByContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationPipeContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionDurationContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionFloatContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionIntegerContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionListContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterAndContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterBooleanContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterHasContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterInContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterKeyEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterKeyNotEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotEqContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotInContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotPrefixContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterNotRegexContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterOrContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterPrefixContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterRegexContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FromContext;
import com.spotify.heroic.grammar.HeroicQueryParser.KeyValueContext;
import com.spotify.heroic.grammar.HeroicQueryParser.QueryContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SelectAllContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SourceRangeAbsoluteContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SourceRangeRelativeContext;
import com.spotify.heroic.grammar.HeroicQueryParser.StringContext;
import com.spotify.heroic.grammar.HeroicQueryParser.WhereContext;
import com.spotify.heroic.metric.MetricType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

@SuppressWarnings("unchecked")
public class QueryListener extends HeroicQueryBaseListener {
    private static final Object KEY_VALUES_MARK = new ObjectMark("KEY_VALUES_MARK");
    private static final Object LIST_MARK = new ObjectMark("LIST_MARK");
    private static final Object EXPR_FUNCTION_ENTER = new ObjectMark("EXPR_FUNCTION_ENTER");

    public static final Object EMPTY = new ObjectMark("EMPTY");
    public static final Object NOT_EMPTY = new ObjectMark("NOT_EMPTY");

    public QueryListener() {
    }

    enum QueryMark {
        QUERY, SELECT, WHERE, FROM, WITH, AS
    }

    public static final Object PIPE_MARK = new ObjectMark("PIPE_MARK");

    public static final Object STATEMENTS_MARK = new ObjectMark("STATEMENTS_MARK");

    public static final String GROUP = "group";
    public static final String CHAIN = "chain";

    private final Stack<Object> stack = new Stack<>();

    @Override
    public void enterStatements(final HeroicQueryParser.StatementsContext ctx) {
        push(STATEMENTS_MARK);
    }

    @Override
    public void exitStatements(final HeroicQueryParser.StatementsContext ctx) {
        final Context c = context(ctx);
        final List<Expression> expressions = popUntil(c, STATEMENTS_MARK, Expression.class);
        push(new Statements(expressions));
    }

    @Override
    public void exitLetStatement(final HeroicQueryParser.LetStatementContext ctx) {
        final Context c = context(ctx);
        final Expression e = pop(c, Expression.class);
        final ReferenceExpression reference = pop(c, ReferenceExpression.class);
        push(new LetExpression(c, reference, e));
    }

    @Override
    public void exitExpressionReference(final HeroicQueryParser.ExpressionReferenceContext ctx) {
        final Context c = context(ctx);
        final String name = ctx.getChild(0).getText().substring(1);
        push(new ReferenceExpression(c, name));
    }

    @Override
    public void exitExpressionDateTime(final HeroicQueryParser.ExpressionDateTimeContext ctx) {
        final Context c = context(ctx);
        push(DateTimeExpression.parse(c, ctx.getChild(1).getText()));
    }

    @Override
    public void exitExpressionTime(final HeroicQueryParser.ExpressionTimeContext ctx) {
        final Context c = context(ctx);
        push(TimeExpression.parse(c, ctx.getChild(1).getText()));
    }

    @Override
    public void enterQuery(QueryContext ctx) {
        push(QueryMark.QUERY);
    }

    @Override
    public void exitQuery(QueryContext ctx) {
        final Context c = context(ctx);

        Optional<Expression> aggregation = Optional.empty();
        Optional<MetricType> source = Optional.empty();
        Optional<RangeExpression> range = Optional.empty();
        Optional<Filter> where = Optional.empty();
        Optional<KeywordValues> with = Optional.empty();
        Optional<KeywordValues> as = Optional.empty();

        outer:
        while (true) {
            final QueryMark mark = pop(c, QueryMark.class);

            switch (mark) {
                case QUERY:
                    break outer;
                case SELECT:
                    aggregation = popOptional(Expression.class);
                    break;
                case WHERE:
                    where = Optional.of(pop(c, Filter.class));
                    break;
                case FROM:
                    source = Optional.of(pop(c, MetricType.class));
                    range = popOptional(RangeExpression.class);
                    break;
                case WITH:
                    with = Optional.of(pop(c, KeywordValues.class));
                    break;
                case AS:
                    as = Optional.of(pop(c, KeywordValues.class));
                    break;
                default:
                    throw c.error(String.format("expected part of query, but got %s", mark));
            }
        }

        if (source == null) {
            throw c.error("No source clause available");
        }

        push(new QueryExpression(c, aggregation, source, range, where,
            with.map(KeywordValues::getMap).orElseGet(ImmutableMap::of),
            as.map(KeywordValues::getMap).orElseGet(ImmutableMap::of)));
    }

    @Override
    public void exitExpressionNegate(
        final HeroicQueryParser.ExpressionNegateContext ctx
    ) {
        final Context c = context(ctx);
        final Expression expression = pop(c, Expression.class);
        push(new NegateExpression(c, expression));
    }

    @Override
    public void exitExpressionPlusMinus(
        final HeroicQueryParser.ExpressionPlusMinusContext ctx
    ) {
        final Context c = context(ctx);
        final Expression right = pop(c, Expression.class);
        final Expression left = pop(c, Expression.class);

        final String operator = ctx.getChild(1).getText();

        switch (operator) {
            case "+":
                push(new PlusExpression(c, left, right));
                break;
            case "-":
                push(new MinusExpression(c, left, right));
                break;
            default:
                throw c.error("Unsupported operator: " + operator);
        }
    }

    @Override
    public void exitExpressionDivMul(
        final HeroicQueryParser.ExpressionDivMulContext ctx
    ) {
        final Context c = context(ctx);
        final Expression right = pop(c, Expression.class);
        final Expression left = pop(c, Expression.class);

        final String operator = ctx.getChild(1).getText();

        switch (operator) {
            case "/":
                push(new DivideExpression(c, left, right));
                break;
            case "*":
                push(new MultiplyExpression(c, left, right));
                break;
            default:
                throw c.error("Unsupported operator: " + operator);
        }
    }

    @Override
    public void exitFilterIn(FilterInContext ctx) {
        final Context c = context(ctx);
        final Expression match = pop(c, Expression.class);
        final StringExpression key = pop(c, StringExpression.class);

        push(OrFilter.create(buildIn(c, key, match)));
    }

    @Override
    public void exitFilterNotIn(FilterNotInContext ctx) {
        final Context c = context(ctx);
        final ListExpression match = pop(c, ListExpression.class);
        final StringExpression key = pop(c, StringExpression.class);

        push(NotFilter.create(OrFilter.create(buildIn(c, key, match))));
    }

    @Override
    public void exitSelectAll(final SelectAllContext ctx) {
        pushOptional(Optional.empty());
        push(QueryMark.SELECT);
    }

    @Override
    public void exitSelectExpression(final HeroicQueryParser.SelectExpressionContext ctx) {
        final Context c = context(ctx);
        final Expression aggregation = pop(c, Expression.class);
        pushOptional(Optional.of(aggregation));
        push(QueryMark.SELECT);
    }

    @Override
    public void exitWhere(WhereContext ctx) {
        final Context c = context(ctx);
        push(pop(c, Filter.class));
        push(QueryMark.WHERE);
    }

    @Override
    public void enterExpressionList(ExpressionListContext ctx) {
        stack.push(LIST_MARK);
    }

    @Override
    public void exitExpressionList(ExpressionListContext ctx) {
        final Context c = context(ctx);
        stack.push(new ListExpression(c, popUntil(c, LIST_MARK, Expression.class)));
    }

    @Override
    public void enterExpressionFunction(HeroicQueryParser.ExpressionFunctionContext ctx) {
        stack.push(EXPR_FUNCTION_ENTER);
    }

    @Override
    public void exitExpressionFunction(final HeroicQueryParser.ExpressionFunctionContext ctx) {
        final Context c = context(ctx);

        final String name = ctx.getChild(0).getText();

        final ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        final ImmutableMap.Builder<String, Expression> keywords = ImmutableMap.builder();

        while (stack.peek() != EXPR_FUNCTION_ENTER) {
            final Object top = stack.pop();

            if (top instanceof KeywordValue) {
                final KeywordValue kw = (KeywordValue) top;
                keywords.put(kw.key, kw.expression);
                continue;
            }

            if (top instanceof Expression) {
                arguments.add((Expression) top);
                continue;
            }

            throw c.error(String.format("expected value, but got %s", top));
        }

        stack.pop();

        push(new FunctionExpression(c, name, Lists.reverse(arguments.build()), keywords.build()));
    }

    @Override
    public void enterKeyValues(final HeroicQueryParser.KeyValuesContext ctx) {
        push(KEY_VALUES_MARK);
    }

    @Override
    public void exitKeyValues(final HeroicQueryParser.KeyValuesContext ctx) {
        final Context c = context(ctx);

        final ImmutableMap.Builder<String, Expression> values = ImmutableMap.builder();

        popUntil(c, KEY_VALUES_MARK, KeywordValue.class).forEach(
            kv -> values.put(kv.getKey(), kv.getExpression()));

        push(new KeywordValues(values.build()));
    }

    @Override
    public void exitKeyValue(KeyValueContext ctx) {
        final Expression expression = pop(context(ctx), Expression.class);
        stack.push(new KeywordValue(ctx.getChild(0).getText(), expression));
    }

    @Override
    public void exitFrom(FromContext ctx) {
        final Context context = context(ctx);

        final String sourceText = ctx.getChild(1).getText();

        final MetricType source = MetricType
            .fromIdentifier(sourceText)
            .orElseThrow(() -> context.error("Invalid source (" + sourceText +
                "), must be one of " + MetricType.values()));

        final Optional<RangeExpression> range;

        if (ctx.getChildCount() > 2) {
            range = Optional.of(pop(context, RangeExpression.class));
        } else {
            range = Optional.empty();
        }

        pushOptional(range);
        push(source);
        push(QueryMark.FROM);
    }

    @Override
    public void exitWith(HeroicQueryParser.WithContext ctx) {
        push(QueryMark.WITH);
    }

    @Override
    public void exitAs(final HeroicQueryParser.AsContext ctx) {
        push(QueryMark.AS);
    }

    @Override
    public void exitSourceRangeAbsolute(SourceRangeAbsoluteContext ctx) {
        final Context c = context(ctx);
        final Expression end = pop(c, Expression.class);
        final Expression start = pop(c, Expression.class);
        push(new RangeExpression(c, start, end));
    }

    @Override
    public void exitSourceRangeRelative(SourceRangeRelativeContext ctx) {
        final Context c = context(ctx);
        final ReferenceExpression now = new ReferenceExpression(c, "now");
        final Expression distance = pop(c, Expression.class);
        final Expression start = new MinusExpression(c, now, distance);
        push(new RangeExpression(c, start, now));
    }

    @Override
    public void exitExpressionInteger(ExpressionIntegerContext ctx) {
        push(new IntegerExpression(context(ctx), Long.parseLong(ctx.getText())));
    }

    @Override
    public void exitExpressionFloat(ExpressionFloatContext ctx) {
        push(new DoubleExpression(context(ctx), Double.parseDouble(ctx.getText())));
    }

    @Override
    public void exitString(StringContext ctx) {
        final ParseTree child = ctx.getChild(0);
        final CommonToken token = (CommonToken) child.getPayload();
        final Context c = context(ctx);

        if (token.getType() == HeroicQueryLexer.SimpleString ||
            token.getType() == HeroicQueryLexer.Identifier) {
            push(new StringExpression(c, child.getText()));
            return;
        }

        push(new StringExpression(c, parseQuotedString(c, child.getText())));
    }

    @Override
    public void exitExpressionDuration(ExpressionDurationContext ctx) {
        final String text = ctx.getText();

        final int value;
        final TimeUnit unit;

        final Context c = context(ctx);

        if (text.length() > 2 && "ms".equals(text.substring(text.length() - 2, text.length()))) {
            unit = TimeUnit.MILLISECONDS;
            value = Integer.parseInt(text.substring(0, text.length() - 2));
        } else {
            unit = extractUnit(c, text.substring(text.length() - 1, text.length()));
            value = Integer.parseInt(text.substring(0, text.length() - 1));
        }

        push(new DurationExpression(c, unit, value));
    }

    @Override
    public void exitFilterHas(FilterHasContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);

        push(HasTagFilter.create(value.getString()));
    }

    @Override
    public void exitFilterNot(FilterNotContext ctx) {
        push(NotFilter.create(pop(context(ctx), Filter.class)));
    }

    @Override
    public void exitFilterKeyEq(FilterKeyEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);

        push(MatchKeyFilter.create(value.getString()));
    }

    @Override
    public void exitFilterKeyNotEq(FilterKeyNotEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);

        push(NotFilter.create(MatchKeyFilter.create(value.getString())));
    }

    @Override
    public void exitFilterEq(FilterEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(MatchTagFilter.create(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotEq(FilterNotEqContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(NotFilter.create(MatchTagFilter.create(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterPrefix(FilterPrefixContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(StartsWithFilter.create(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotPrefix(FilterNotPrefixContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(NotFilter.create(StartsWithFilter.create(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterRegex(FilterRegexContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(RegexFilter.create(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotRegex(FilterNotRegexContext ctx) {
        final StringExpression value = pop(context(ctx), StringExpression.class);
        final StringExpression key = pop(context(ctx), StringExpression.class);

        push(NotFilter.create(RegexFilter.create(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterAnd(FilterAndContext ctx) {
        final Context c = context(ctx);
        final Filter b = pop(c, Filter.class);
        final Filter a = pop(c, Filter.class);
        push(AndFilter.create(ImmutableList.of(a, b)));
    }

    @Override
    public void exitFilterOr(FilterOrContext ctx) {
        final Context c = context(ctx);
        final Filter b = pop(c, Filter.class);
        final Filter a = pop(c, Filter.class);
        push(OrFilter.create(ImmutableList.of(a, b)));
    }

    @Override
    public void exitAggregationBy(final AggregationByContext ctx) {
        final Context c = context(ctx);

        final Expression group = pop(c, Expression.class);
        final FunctionExpression left = pop(c, Expression.class).cast(FunctionExpression.class);

        push(new FunctionExpression(c, GROUP, ImmutableList.of(group, left), ImmutableMap.of()));
    }

    @Override
    public void exitAggregationByAll(final AggregationByAllContext ctx) {
        final Context c = context(ctx);

        final FunctionExpression left = pop(c, Expression.class).cast(FunctionExpression.class);

        push(new FunctionExpression(c, GROUP, ImmutableList.of(new EmptyExpression(c), left),
            ImmutableMap.of()));
    }

    @Override
    public void enterAggregationPipe(AggregationPipeContext ctx) {
        stack.push(PIPE_MARK);
    }

    @Override
    public void exitAggregationPipe(AggregationPipeContext ctx) {
        final Context c = context(ctx);
        final List<Expression> values =
            ImmutableList.copyOf(popUntil(c, PIPE_MARK, Expression.class).stream().iterator());
        push(new FunctionExpression(c, CHAIN, values, ImmutableMap.of()));
    }

    @Override
    public void exitFilterBoolean(FilterBooleanContext ctx) {
        final Context c = context(ctx);
        final String literal = ctx.getText();

        if ("true".equals(literal)) {
            push(TrueFilter.get());
            return;
        }

        if ("false".equals(literal)) {
            push(FalseFilter.get());
            return;
        }

        throw c.error("unsupported boolean literal: " + literal);
    }

    private <T> List<T> popUntil(final Context c, final Object mark, final Class<T> type) {
        final ImmutableList.Builder<T> results = ImmutableList.builder();

        while (stack.peek() != mark) {
            results.add(pop(c, type));
        }

        stack.pop();
        return Lists.reverse(results.build());
    }

    private List<Filter> buildIn(
        final Context c, final StringExpression key, final Expression match
    ) {
        if (match instanceof StringExpression) {
            return ImmutableList.of(MatchTagFilter.create(key.getString(),
                match.cast(StringExpression.class).getString()));
        }

        if (!(match instanceof ListExpression)) {
            throw c.error("Cannot use type " + match + " in expression");
        }

        final List<Filter> values = new ArrayList<>();

        for (final Expression v : ((ListExpression) match).getList()) {
            values.add(
                MatchTagFilter.create(key.getString(), v.cast(StringExpression.class).getString()));
        }

        return values;
    }

    private TimeUnit extractUnit(Context ctx, String text) {
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

    public <T> T pop(Class<T> type) {
        if (stack.isEmpty()) {
            throw new IllegalStateException("Parse stack is empty");
        }

        final Object popped = stack.pop();
        checkType(type, popped.getClass());
        return (T) popped;
    }

    public <T extends Enum<T>> void popMark(T mark) {
        final T actual = pop(mark.getDeclaringClass());

        if (actual != mark) {
            throw new IllegalStateException("Expected mark " + mark + ", but got " + actual);
        }
    }

    public <T> Optional<T> popOptional(Class<T> type) {
        final Object mark = stack.pop();

        if (mark == EMPTY) {
            return Optional.empty();
        }

        if (mark == NOT_EMPTY) {
            return Optional.of(pop(type));
        }

        throw new IllegalStateException("stack does not contain a legal optional mark");
    }

    private <T> void checkType(Class<T> expected, Class<?> actual) {
        if (!expected.isAssignableFrom(actual)) {
            throw new IllegalStateException(
                String.format("expected %s, but was %s (rest: %s)", name(expected), actual, stack));
        }
    }

    /* internals */
    private <T> void pushOptional(final Optional<T> value) {
        if (!value.isPresent()) {
            push(EMPTY);
            return;
        }

        push(value.get());
        push(NOT_EMPTY);
    }

    private void push(Object value) {
        stack.push(Objects.requireNonNull(value));
    }

    private <T> T pop(Context ctx, Class<T> type) {
        if (stack.isEmpty()) {
            throw ctx.error(String.format("expected %s, but was empty", name(type)));
        }

        final Object popped = stack.pop();
        checkType(type, popped.getClass());
        return (T) popped;
    }

    private static Context context(final ParserRuleContext source) {
        int line = source.getStart().getLine() - 1;
        int col = source.getStart().getStartIndex();
        int lineEnd = source.getStop().getLine() - 1;
        int colEnd = source.getStop().getStopIndex();
        return new Context(line, col, lineEnd, colEnd);
    }

    private static String name(Class<?> type) {
        final JsonTypeName name = type.getAnnotation(JsonTypeName.class);

        if (name == null) {
            return type.getName();
        }

        return "<" + name.value() + ">";
    }

    @Data
    static final class Statements {
        private final List<Expression> expressions;
    }

    @Data
    static final class KeywordValue {
        private final String key;
        private final Expression expression;
    }

    @Data
    static final class KeywordValues {
        private final Map<String, Expression> map;
    }

    static class ObjectMark {
        private final String name;

        @java.beans.ConstructorProperties({ "name" })
        public ObjectMark(final String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
