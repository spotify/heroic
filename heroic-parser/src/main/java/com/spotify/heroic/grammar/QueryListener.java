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
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.spotify.heroic.Query;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationByAllContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationByContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationPipeContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionDurationContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionIntegerContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionListContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionMinusContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionNowContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ExpressionPlusContext;
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
import com.spotify.heroic.grammar.HeroicQueryParser.QueriesContext;
import com.spotify.heroic.grammar.HeroicQueryParser.QueryContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SelectAggregationContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SelectAllContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SourceRangeAbsoluteContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SourceRangeRelativeContext;
import com.spotify.heroic.grammar.HeroicQueryParser.StringContext;
import com.spotify.heroic.grammar.HeroicQueryParser.WhereContext;
import com.spotify.heroic.metric.MetricType;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@SuppressWarnings("unchecked")
@RequiredArgsConstructor
public class QueryListener extends HeroicQueryBaseListener {
    private final FilterFactory filters;
    private final AggregationFactory aggregations;
    private final long now;

    private static final Object QUERIES_MARK = new Object();
    private static final Object LIST_MARK = new Object();
    private static final Object AGGREGAITON_DEF = new Object();

    public static final Object EMPTY = new Object();
    public static final Object NOT_EMPTY = new Object();

    public static final Object QUERY_MARK = new Object();
    public static final Object GROUP_BY_MARK = new Object();
    public static final Object SELECT_MARK = new Object();
    public static final Object WHERE_MARK = new Object();
    public static final Object FROM_MARK = new Object();

    public static final Object PIPE_MARK = new Object();

    public static final String GROUP = "group";
    public static final String CHAIN = "chain";

    private final Stack<Object> stack = new Stack<>();

    @Override
    public void enterQueries(QueriesContext ctx) {
        push(QUERIES_MARK);
    }

    @Override
    public void exitQueries(QueriesContext ctx) {
        final Context c = context(ctx);

        final List<Query> list = new ArrayList<>();

        while (true) {
            if (stack.peek() == QUERIES_MARK) {
                stack.pop();
                break;
            }

            list.add(pop(c, Query.class));
        }

        push(new Queries(list));
    }

    @Override
    public void enterQuery(QueryContext ctx) {
        push(QUERY_MARK);
    }

    @Override
    public void exitQuery(QueryContext ctx) {
        Optional<Aggregation> aggregation = Optional.empty();
        Optional<MetricType> source = Optional.empty();
        Optional<QueryDateRange> range = Optional.empty();
        Optional<Filter> where = Optional.empty();

        final Context c = context(ctx);

        while (true) {
            final Object mark = stack.pop();

            if (mark == QUERY_MARK) {
                break;
            }

            if (mark == SELECT_MARK) {
                aggregation = popOptional(Aggregation.class);
                continue;
            }

            if (mark == WHERE_MARK) {
                where = Optional.of(pop(Filter.class));
                continue;
            }

            if (mark == FROM_MARK) {
                source = Optional.of(pop(MetricType.class));
                range = popOptional(QueryDateRange.class);
                continue;
            }

            throw c.error(String.format("expected part of query, but got %s", mark));
        }

        if (source == null) {
            throw c.error("No source clause available");
        }

        push(new Query(aggregation, source, range, where, Optional.empty(), Optional.empty()));
    }

    @Override
    public void exitExpressionNow(ExpressionNowContext ctx) {
        push(new IntValue(now, context(ctx)));
    }

    @Override
    public void exitExpressionMinus(ExpressionMinusContext ctx) {
        final Context c = context(ctx);
        final Value b = pop(c, Value.class);
        final Value a = pop(c, Value.class);
        push(a.sub(b));
    }

    @Override
    public void exitExpressionPlus(ExpressionPlusContext ctx) {
        final Context c = context(ctx);
        final Value b = pop(c, Value.class);
        final Value a = pop(c, Value.class);
        push(a.add(b));
    }

    @Override
    public void exitFilterIn(FilterInContext ctx) {
        final Context c = context(ctx);
        final Value match = pop(c, Value.class);
        final StringValue key = pop(c, StringValue.class);

        push(filters.or(buildIn(c, key, match)));
    }

    @Override
    public void exitFilterNotIn(FilterNotInContext ctx) {
        final Context c = context(ctx);
        final ListValue match = pop(c, ListValue.class);
        final StringValue key = pop(c, StringValue.class);

        push(filters.not(filters.or(buildIn(c, key, match))));
    }

    @Override
    public void exitSelectAll(final SelectAllContext ctx) {
        pushOptional(Optional.empty());
        push(SELECT_MARK);
    }

    @Override
    public void exitSelectAggregation(final SelectAggregationContext ctx) {
        final Context c = context(ctx);
        pushOptional(pop(c, Value.class).toOptional()
                .map(o -> o.cast(AggregationValue.class).build(aggregations)));
        push(SELECT_MARK);
    }

    @Override
    public void exitWhere(WhereContext ctx) {
        final Context c = context(ctx);
        push(pop(c, Filter.class).optimize());
        push(WHERE_MARK);
    }

    @Override
    public void enterExpressionList(ExpressionListContext ctx) {
        stack.push(LIST_MARK);
    }

    @Override
    public void exitExpressionList(ExpressionListContext ctx) {
        final Context c = context(ctx);
        stack.push(new ListValue(popUntil(c, LIST_MARK, Value.class), c));
    }

    @Override
    public void enterAggregation(AggregationContext ctx) {
        stack.push(AGGREGAITON_DEF);
    }

    @Override
    public void exitAggregation(final AggregationContext ctx) {
        final Context c = context(ctx);

        final String name = ctx.getChild(0).getText();

        final ImmutableList.Builder<Value> arguments = ImmutableList.builder();
        final ImmutableMap.Builder<String, Value> keywords = ImmutableMap.builder();

        while (stack.peek() != AGGREGAITON_DEF) {
            final Object top = stack.pop();

            if (top instanceof KeywordValue) {
                final KeywordValue kw = (KeywordValue) top;
                keywords.put(kw.key, kw.value);
                continue;
            }

            if (top instanceof Value) {
                arguments.add((Value) top);
                continue;
            }

            throw c.error(String.format("expected value, but got %s", top));
        }

        stack.pop();

        push(new AggregationValue(name, new ListValue(Lists.reverse(arguments.build()), c),
                keywords.build(), c));
    }

    @Override
    public void exitKeyValue(KeyValueContext ctx) {
        final Value value = pop(context(ctx), Value.class);
        stack.push(new KeywordValue(ctx.getChild(0).getText(), value));
    }

    @Override
    public void exitFrom(FromContext ctx) {
        final Context context = context(ctx);

        final String sourceText = ctx.getChild(1).getText();

        final MetricType source = MetricType.fromIdentifier(sourceText)
                .orElseThrow(() -> context.error("Invalid source (" + sourceText
                        + "), must be one of " + MetricType.values()));

        final Optional<QueryDateRange> range;

        if (ctx.getChildCount() > 2) {
            range = Optional.of(pop(context, QueryDateRange.class));
        } else {
            range = Optional.empty();
        }

        pushOptional(range);
        push(source);
        push(FROM_MARK);
    }

    @Override
    public void exitSourceRangeAbsolute(SourceRangeAbsoluteContext ctx) {
        final IntValue end = pop(context(ctx), IntValue.class);
        final IntValue start = pop(context(ctx), IntValue.class);
        push(new QueryDateRange.Absolute(start.getValue(), end.getValue()));
    }

    @Override
    public void exitSourceRangeRelative(SourceRangeRelativeContext ctx) {
        final DurationValue diff = pop(context(ctx), DurationValue.class);
        push(new QueryDateRange.Relative(diff.getUnit(), diff.getValue()));
    }

    @Override
    public void exitExpressionInteger(ExpressionIntegerContext ctx) {
        push(new IntValue(Long.parseLong(ctx.getText()), context(ctx)));
    }

    @Override
    public void exitString(StringContext ctx) {
        final ParseTree child = ctx.getChild(0);
        final CommonToken token = (CommonToken) child.getPayload();
        final Context c = context(ctx);

        if (token.getType() == HeroicQueryLexer.SimpleString
                || token.getType() == HeroicQueryLexer.Identifier) {
            push(new StringValue(child.getText(), c));
            return;
        }

        push(new StringValue(parseQuotedString(child.getText()), c));
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

        push(new DurationValue(unit, value, c));
    }

    @Override
    public void exitFilterHas(FilterHasContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);

        push(filters.hasTag(value.getString()));
    }

    @Override
    public void exitFilterNot(FilterNotContext ctx) {
        push(filters.not(pop(context(ctx), Filter.class)));
    }

    @Override
    public void exitFilterKeyEq(FilterKeyEqContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);

        push(filters.matchKey(value.getString()));
    }

    @Override
    public void exitFilterKeyNotEq(FilterKeyNotEqContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);

        push(filters.not(filters.matchKey(value.getString())));
    }

    @Override
    public void exitFilterEq(FilterEqContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);
        final StringValue key = pop(context(ctx), StringValue.class);

        push(filters.matchTag(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotEq(FilterNotEqContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);
        final StringValue key = pop(context(ctx), StringValue.class);

        push(filters.not(filters.matchTag(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterPrefix(FilterPrefixContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);
        final StringValue key = pop(context(ctx), StringValue.class);

        push(filters.startsWith(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotPrefix(FilterNotPrefixContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);
        final StringValue key = pop(context(ctx), StringValue.class);

        push(filters.not(filters.startsWith(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterRegex(FilterRegexContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);
        final StringValue key = pop(context(ctx), StringValue.class);

        push(filters.regex(key.getString(), value.getString()));
    }

    @Override
    public void exitFilterNotRegex(FilterNotRegexContext ctx) {
        final StringValue value = pop(context(ctx), StringValue.class);
        final StringValue key = pop(context(ctx), StringValue.class);

        push(filters.not(filters.regex(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterAnd(FilterAndContext ctx) {
        final Context c = context(ctx);
        final Filter b = pop(c, Filter.class);
        final Filter a = pop(c, Filter.class);
        push(filters.and(a, b));
    }

    @Override
    public void exitFilterOr(FilterOrContext ctx) {
        final Context c = context(ctx);
        final Filter b = pop(c, Filter.class);
        final Filter a = pop(c, Filter.class);
        push(filters.or(a, b));
    }

    @Override
    public void exitAggregationBy(final AggregationByContext ctx) {
        final Context c = context(ctx);

        final ListValue group = pop(c, Value.class).cast(ListValue.class);
        final AggregationValue left = pop(c, Value.class).cast(AggregationValue.class);

        push(new AggregationValue(GROUP, Value.list(group, left), ImmutableMap.of(), c));
    }

    @Override
    public void exitAggregationByAll(final AggregationByAllContext ctx) {
        final Context c = context(ctx);

        final AggregationValue left = pop(c, Value.class).cast(AggregationValue.class);

        push(new AggregationValue(GROUP, Value.list(new EmptyValue(c), left), ImmutableMap.of(),
                c));
    }

    @Override
    public void enterAggregationPipe(AggregationPipeContext ctx) {
        stack.push(PIPE_MARK);
    }

    @Override
    public void exitAggregationPipe(AggregationPipeContext ctx) {
        final Context c = context(ctx);
        final List<AggregationValue> values =
                ImmutableList.copyOf(popUntil(c, PIPE_MARK, Value.class).stream()
                        .map(v -> v.cast(AggregationValue.class)).iterator());
        push(new AggregationValue(CHAIN, new ListValue(values, c), ImmutableMap.of(), c));
    }

    @Override
    public void exitFilterBoolean(FilterBooleanContext ctx) {
        final Context c = context(ctx);
        final String literal = ctx.getText();

        if ("true".equals(literal)) {
            push(filters.t());
            return;
        }

        if ("false".equals(literal)) {
            push(filters.f());
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

    private List<Filter> buildIn(final Context c, final StringValue key, final Value match) {
        if (match instanceof StringValue) {
            return ImmutableList.of(filters.matchTag(key.getString(), match.cast(String.class)));
        }

        if (!(match instanceof ListValue)) {
            throw c.error("Cannot use type " + match + " in expression");
        }

        final List<Filter> values = new ArrayList<>();

        for (final Value v : ((ListValue) match).getList()) {
            values.add(filters.matchTag(key.getString(), v.cast(String.class)));
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

    private String parseQuotedString(String text) {
        int i = 0;
        boolean escapeNext = false;

        final StringBuilder builder = new StringBuilder();

        while (i < text.length()) {
            final char c = text.charAt(i++);

            if (i == 1 || i == text.length()) {
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

        return builder.toString();
    }

    public void popMark(Object mark) {
        final Object actual = pop(Object.class);

        if (actual != mark) {
            throw new IllegalStateException("Expected mark " + mark + ", but got " + actual);
        }
    }

    public <T> List<T> popList(Class<T> type) {
        final Class<?> typeOn = pop(Class.class);
        checkType(type, typeOn);
        return (List<T>) pop(List.class);
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

    public <T> T pop(Class<T> type) {
        if (stack.isEmpty()) {
            throw new IllegalStateException("stack is empty (did you parse something?)");
        }

        final Object popped = stack.pop();

        checkType(type, popped.getClass());

        return (T) popped;
    }

    private <T> void checkType(Class<T> expected, Class<?> actual) {
        if (!expected.isAssignableFrom(actual)) {
            throw new IllegalStateException(
                    String.format("expected %s, but was %s", name(expected), name(actual)));
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

    @Data
    private static final class KeywordValue {
        private final String key;
        private final Value value;
    }

    private static Context context(final ParserRuleContext source) {
        int line = source.getStart().getLine();
        int col = source.getStart().getCharPositionInLine();
        int lineEnd = source.getStop().getLine();
        int colEnd = source.getStop().getCharPositionInLine();
        return new Context(line, col, lineEnd, colEnd);
    }

    private static String name(Class<?> type) {
        final ValueName name = type.getAnnotation(ValueName.class);

        if (name == null) {
            return type.getName();
        }

        return "<" + name.value() + ">";
    }
}
