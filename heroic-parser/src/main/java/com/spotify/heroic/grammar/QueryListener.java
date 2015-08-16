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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.HeroicQueryParser.AbsoluteContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationArgsContext;
import com.spotify.heroic.grammar.HeroicQueryParser.AggregationContext;
import com.spotify.heroic.grammar.HeroicQueryParser.DiffContext;
import com.spotify.heroic.grammar.HeroicQueryParser.EqExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FilterExprsContext;
import com.spotify.heroic.grammar.HeroicQueryParser.FromContext;
import com.spotify.heroic.grammar.HeroicQueryParser.GroupByContext;
import com.spotify.heroic.grammar.HeroicQueryParser.HasExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.InExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.IntegerContext;
import com.spotify.heroic.grammar.HeroicQueryParser.KeyEqExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.KeyNotEqExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.KeyValueContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ListContext;
import com.spotify.heroic.grammar.HeroicQueryParser.NotEqExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.NotExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.NotInExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.NotPrefixExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.NotRegexExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.NowContext;
import com.spotify.heroic.grammar.HeroicQueryParser.PrefixExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.QueriesContext;
import com.spotify.heroic.grammar.HeroicQueryParser.QueryContext;
import com.spotify.heroic.grammar.HeroicQueryParser.RegexExprContext;
import com.spotify.heroic.grammar.HeroicQueryParser.RelativeContext;
import com.spotify.heroic.grammar.HeroicQueryParser.SelectContext;
import com.spotify.heroic.grammar.HeroicQueryParser.StringContext;
import com.spotify.heroic.grammar.HeroicQueryParser.ValueExprContext;

@SuppressWarnings("unchecked")
@RequiredArgsConstructor
public class QueryListener extends HeroicQueryBaseListener {
    private final FilterFactory filters;
    private final long now;

    private static final Object QUERIES_MARK = new Object();
    private static final Object GROUPBY_MARK = new Object();
    private static final Object LIST_MARK = new Object();
    private static final Object AGGREGATION_ARGUMENTS_MARK = new Object();
    private static final Object QUERY_MARK = new Object();

    @Getter
    private final List<QueryDSL> queries = new ArrayList<>();
    private final Stack<Object> stack = new Stack<>();

    @Override
    public void enterQueries(QueriesContext ctx) {
        push(QUERIES_MARK);
    }

    @Override
    public void exitQueries(QueriesContext ctx) {
        final ContextImpl c = new ContextImpl(ctx);

        final List<QueryDSL> list = new ArrayList<>();

        while (true) {
            if (stack.peek() == QUERIES_MARK) {
                stack.pop();
                break;
            }

            list.add(pop(c, QueryDSL.class));
        }

        push(new Queries(list));
    }

    @Override
    public void enterQuery(QueryContext ctx) {
        push(QUERY_MARK);
    }

    @Override
    public void exitQuery(QueryContext ctx) {
        SelectDSL select = null;
        FromDSL source = null;
        Filter where = null;
        GroupByDSL groupBy = null;

        final Context c = new ContextImpl(ctx);

        while (true) {
            final Object mark = stack.pop();

            if (mark == QUERY_MARK) {
                break;
            }

            if (mark instanceof SelectDSL) {
                select = (SelectDSL) mark;
                continue;
            }

            if (mark instanceof FromDSL) {
                source = (FromDSL) mark;
                continue;
            }

            if (mark instanceof Filter) {
                where = (Filter) mark;
                continue;
            }

            if (mark instanceof GroupByDSL) {
                groupBy = (GroupByDSL) mark;
                continue;
            }

            throw c.error(String.format("expected part of query, but got %s", mark));
        }

        if (select == null) {
            throw c.error("No select clause available");
        }

        if (source == null) {
            throw c.error("No source clause available");
        }

        push(new QueryDSL(select, source, Optional.fromNullable(where), Optional.fromNullable(groupBy)));
    }

    @Override
    public void exitNow(NowContext ctx) {
        push(new IntValue(now));
    }

    @Override
    public void exitValueExpr(ValueExprContext ctx) {
        if (ctx.getChildCount() == 1)
            return;

        final ContextImpl c = new ContextImpl(ctx);

        final Value right = pop(c, Value.class);
        final Value left = pop(c, Value.class);

        final ParseTree child = ctx.getChild(1);

        if (child.getPayload() instanceof CommonToken) {
            final CommonToken op = (CommonToken) child.getPayload();

            try {
                if (op.getType() == HeroicQueryLexer.Plus) {
                    push(left.add(right));
                    return;
                }

                if (op.getType() == HeroicQueryLexer.Minus) {
                    push(left.sub(right));
                    return;
                }
            } catch (Exception e) {
                throw c.error(e);
            }
        }

        throw c.error("invalid operation: " + child.getText());
    }

    @Override
    public void exitInExpr(InExprContext ctx) {
        final ContextImpl c = new ContextImpl(ctx);

        final ListValue list = pop(c, ListValue.class);
        final StringValue key = pop(c, StringValue.class);

        final List<Filter> values = new ArrayList<>();

        for (final Value v : list.getList())
            values.add(filters.matchTag(key.getString(), v.cast(String.class)));

        push(filters.or(values));
    }

    @Override
    public void exitNotInExpr(NotInExprContext ctx) {
        final ContextImpl c = new ContextImpl(ctx);

        final ListValue list = pop(c, ListValue.class);
        final StringValue key = pop(c, StringValue.class);

        final List<Filter> values = new ArrayList<>();

        for (final Value v : list.getList())
            values.add(filters.matchTag(key.getString(), v.cast(String.class)));

        push(filters.not(filters.or(values)));
    }

    @Override
    public void exitSelect(SelectContext ctx) {
        final ParseTree child = ctx.getChild(0);
        final Context c = new ContextImpl(ctx);
        final Optional<AggregationValue> aggregation = buildAggregation(c, child);
        push(new SelectDSL(c, aggregation));
    }

    Optional<AggregationValue> buildAggregation(final Context c, final ParseTree child) {
        if (!(child.getPayload() instanceof ValueExprContext)) {
            return Optional.absent();
        }

        final Value value = pop(c, Value.class);

        if (value instanceof AggregationValue) {
            return Optional.of((AggregationValue) value);
        }

        if (value instanceof StringValue) {
            final StringValue name = (StringValue) value;
            final List<Value> arguments = new ArrayList<>();
            final Map<String, Value> keywordArguments = new HashMap<>();
            return Optional.of(new AggregationValue(name.getString(), arguments, keywordArguments));
        }

        throw c.error(String.format("expected aggregation, but was %s", name(AggregationValue.class), value.toString()));
    }

    @Override
    public void exitFilter(FilterContext ctx) {
        final Context c = new ContextImpl(ctx);
        push(pop(c, Filter.class).optimize());
    }

    @Override
    public void enterGroupBy(GroupByContext ctx) {
        stack.push(GROUPBY_MARK);
    }

    @Override
    public void exitGroupBy(GroupByContext ctx) {
        final List<String> list = new ArrayList<>();

        while (true) {
            if (stack.peek() == GROUPBY_MARK) {
                stack.pop();
                break;
            }

            list.add(pop(new ContextImpl(ctx), StringValue.class).getString());
        }

        Collections.reverse(list);
        push(new GroupByDSL(list));
    }

    @Override
    public void enterList(ListContext ctx) {
        stack.push(LIST_MARK);
    }

    @Override
    public void exitList(ListContext ctx) {
        final List<Value> list = new ArrayList<>();

        final Context c = new ContextImpl(ctx);

        while (true) {
            if (stack.peek() == LIST_MARK) {
                stack.pop();
                break;
            }

            list.add(pop(c, Value.class));
        }

        Collections.reverse(list);

        stack.push(new ListValue(list));
    }

    @Override
    public void enterAggregationArgs(AggregationArgsContext ctx) {
        stack.push(AGGREGATION_ARGUMENTS_MARK);
    }

    @Override
    public void exitAggregationArgs(AggregationArgsContext ctx) {
        final List<Value> arguments = new ArrayList<>();
        final Map<String, Value> keywordArguments = new HashMap<>();

        final Context c = new ContextImpl(ctx);

        while (true) {
            final Object argument = stack.pop();

            if (argument == AGGREGATION_ARGUMENTS_MARK) {
                break;
            }

            if (argument instanceof KeywordValue) {
                final KeywordValue kw = (KeywordValue) argument;
                keywordArguments.put(kw.key, kw.value);
                continue;
            }

            if (!(argument instanceof Value)) {
                throw c.error(String.format("expected value, but got %s", argument));
            }

            arguments.add((Value) argument);
        }

        Collections.reverse(arguments);
        push(new AggregationArguments(arguments, keywordArguments));
    }

    @Override
    public void exitAggregation(AggregationContext ctx) {
        final Context c = new ContextImpl(ctx);

        final AggregationArguments arguments;

        if (ctx.getChildCount() == 4) {
            arguments = pop(c, AggregationArguments.class);
        } else {
            arguments = AggregationArguments.empty();
        }

        final StringValue name = pop(c, StringValue.class);
        push(new AggregationValue(name.getString(), arguments.getPositional(), arguments.getKeywords()));
    }

    @Override
    public void exitKeyValue(KeyValueContext ctx) {
        final Value value = pop(new ContextImpl(ctx), Value.class);
        stack.push(new KeywordValue(ctx.getChild(0).getText(), value));
    }

    @Override
    public void exitFrom(FromContext ctx) {
        final ParseTree source = ctx.getChild(0);

        final Context context = new ContextImpl(ctx);

        final Optional<DateRange> range;

        if (ctx.getChildCount() > 1) {
            range = Optional.of(pop(context, DateRange.class));
        } else {
            range = Optional.absent();
        }

        push(new FromDSL(context, source.getText(), range));
    }

    @Override
    public void exitAbsolute(AbsoluteContext ctx) {
        final IntValue end = pop(new ContextImpl(ctx), IntValue.class);
        final IntValue start = pop(new ContextImpl(ctx), IntValue.class);
        push(new DateRange(start.getValue(), end.getValue()));
    }

    @Override
    public void exitRelative(RelativeContext ctx) {
        final DiffValue diff = pop(new ContextImpl(ctx), DiffValue.class);
        final long ms = TimeUnit.MILLISECONDS.convert(diff.getValue(), diff.getUnit());
        push(new DateRange(now - ms, now));
    }

    @Override
    public void exitInteger(IntegerContext ctx) {
        push(new IntValue(Long.valueOf(ctx.getText())));
    }

    @Override
    public void exitString(StringContext ctx) {
        final ParseTree child = ctx.getChild(0);
        final CommonToken token = (CommonToken) child.getPayload();

        if (token.getType() == HeroicQueryLexer.SimpleString || token.getType() == HeroicQueryLexer.Identifier) {
            push(new StringValue(child.getText()));
            return;
        }

        push(new StringValue(parseQuotedString(child.getText())));
    }

    @Override
    public void exitDiff(DiffContext ctx) {
        final String text = ctx.getText();

        final int value;
        final TimeUnit unit;

        final Context c = new ContextImpl(ctx);

        if (text.length() > 2 && "ms".equals(text.substring(text.length() - 2, text.length()))) {
            unit = TimeUnit.MILLISECONDS;
            value = Integer.valueOf(text.substring(0, text.length() - 2));
        } else {
            unit = extractUnit(c, text.substring(text.length() - 1, text.length()));
            value = Integer.valueOf(text.substring(0, text.length() - 1));
        }

        push(new DiffValue(unit, value));
    }

    @Override
    public void exitHasExpr(HasExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.hasTag(value.getString()));
    }

    @Override
    public void exitNotExpr(NotExprContext ctx) {
        push(filters.not(pop(new ContextImpl(ctx), Filter.class)));
    }

    @Override
    public void exitKeyEqExpr(KeyEqExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.matchKey(value.getString()));
    }

    @Override
    public void exitKeyNotEqExpr(KeyNotEqExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.not(filters.matchKey(value.getString())));
    }

    @Override
    public void exitEqExpr(EqExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        final StringValue key = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.matchTag(key.getString(), value.getString()));
    }

    @Override
    public void exitNotEqExpr(NotEqExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        final StringValue key = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.not(filters.matchTag(key.getString(), value.getString())));
    }

    @Override
    public void exitPrefixExpr(PrefixExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        final StringValue key = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.startsWith(key.getString(), value.getString()));
    }

    @Override
    public void exitNotPrefixExpr(NotPrefixExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        final StringValue key = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.not(filters.startsWith(key.getString(), value.getString())));
    }

    @Override
    public void exitRegexExpr(RegexExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        final StringValue key = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.regex(key.getString(), value.getString()));
    }

    @Override
    public void exitNotRegexExpr(NotRegexExprContext ctx) {
        final StringValue value = pop(new ContextImpl(ctx), StringValue.class);
        final StringValue key = pop(new ContextImpl(ctx), StringValue.class);
        push(filters.not(filters.regex(key.getString(), value.getString())));
    }

    @Override
    public void exitFilterExprs(FilterExprsContext ctx) {
        // a single filter should already be on the stack, regardless of expression type.
        if (ctx.getChildCount() == 1)
            return;

        final Context c = new ContextImpl(ctx);

        final String type = ctx.getChild(1).getText();

        final Filter a = pop(c, Filter.class);
        final Filter b = pop(c, Filter.class);

        if ("and".equals(type)) {
            push(filters.and(a, b));
            return;
        }

        if ("or".equals(type)) {
            push(filters.or(a, b));
            return;
        }

        throw c.error("unsupported expression: " + type);
    }

    private TimeUnit extractUnit(Context ctx, String text) {
        if ("s".equals(text))
            return TimeUnit.SECONDS;

        if ("m".equals(text))
            return TimeUnit.MINUTES;

        if ("H".equals(text))
            return TimeUnit.HOURS;

        if ("d".equals(text))
            return TimeUnit.DAYS;

        throw ctx.error("illegal unit: " + text);
    }

    private String parseQuotedString(String text) {
        int i = 0;
        boolean escapeNext = false;

        final StringBuilder builder = new StringBuilder();

        while (i < text.length()) {
            final char c = text.charAt(i++);

            if (i == 1 || i == text.length())
                continue;

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

    public <T> T pop(Class<T> type) {
        if (stack.isEmpty())
            throw new IllegalStateException("stack is empty (did you parse something?)");

        final Object popped = stack.pop();

        if (!type.isAssignableFrom(popped.getClass()))
            throw new IllegalStateException(String.format("expected %s, but was %s", name(type),
                    name(popped.getClass())));

        return (T) popped;
    }

    /* internals */

    private void push(Object value) {
        stack.push(value);
    }

    private <T> T pop(Context ctx, Class<T> type) {
        final Object popped = stack.pop();

        if (popped == null)
            throw ctx.error(String.format("expected %s, but was empty", name(type)));

        if (!type.isAssignableFrom(popped.getClass())) {
            throw ctx.error(String.format("expected %s, but was %s", name(type), popped.toString()));
        }

        return (T) popped;
    }

    @Data
    private static final class KeywordValue {
        private final String key;
        private final Value value;
    }

    @Data
    static final class AggregationArguments {
        final List<Value> positional;
        final Map<String, Value> keywords;

        static final AggregationArguments empty = new AggregationArguments(ImmutableList.of(), ImmutableMap.of());

        public static AggregationArguments empty() {
            return empty;
        }
    }

    @Data
    private static final class ContextImpl implements Context {
        private final ParserRuleContext ctx;

        public RuntimeException error(String message) {
            int line = ctx.getStart().getLine();
            int col = ctx.getStart().getCharPositionInLine();
            int lineEnd = ctx.getStop().getLine();
            int colEnd = ctx.getStop().getCharPositionInLine();
            return new ParseException(message, null, line, col, lineEnd, colEnd);
        }

        @Override
        public RuntimeException error(Exception cause) {
            int line = ctx.getStart().getLine();
            int col = ctx.getStart().getCharPositionInLine();
            int lineEnd = ctx.getStop().getLine();
            int colEnd = ctx.getStop().getCharPositionInLine();
            return new ParseException(cause.getMessage(), cause, line, col, lineEnd, colEnd);
        }
    }

    private static String name(Class<?> type) {
        final ValueName name = type.getAnnotation(ValueName.class);

        if (name == null)
            return type.getName();

        return "<" + name.value() + ">";
    }
}
