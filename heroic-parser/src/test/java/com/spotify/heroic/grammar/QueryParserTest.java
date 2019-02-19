package com.spotify.heroic.grammar;

import static com.spotify.heroic.grammar.Expression.duration;
import static com.spotify.heroic.grammar.Expression.empty;
import static com.spotify.heroic.grammar.Expression.function;
import static com.spotify.heroic.grammar.Expression.integer;
import static com.spotify.heroic.grammar.Expression.let;
import static com.spotify.heroic.grammar.Expression.list;
import static com.spotify.heroic.grammar.Expression.range;
import static com.spotify.heroic.grammar.Expression.reference;
import static com.spotify.heroic.grammar.Expression.string;
import static org.junit.Assert.assertEquals;

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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryParserTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final CoreQueryParser parser = new CoreQueryParser();

    @Test
    public void testString() {
        assertEquals(string(cols(0, 2), "foo"), parseExpression("foo"));
        assertEquals(string(cols(0, 4), "foo"), parseExpression("\"foo\""));
        assertEquals(string(cols(0, 2), "\u4040"), parseExpression("\"\u4040\""));
        assertEquals(string(cols(0, 1), "1a"), parseExpression("1a"));

        // test all possible escape sequences
        assertEquals(string(cols(0, 17), "\b\t\n\f\r\"\'\\"),
            parseExpression("\"\\b\\t\\n\\f\\r\\\"\\'\\\\\""));
    }

    @Test
    public void testStringEscapes() {
        final List<Pair<String, String>> tests =
            ImmutableList.of(Pair.of("b", "\b"), Pair.of("t", "\t"), Pair.of("n", "\n"),
                Pair.of("f", "\f"), Pair.of("r", "\r"), Pair.of("\"", "\""), Pair.of("\'", "'"),
                Pair.of("\\", "\\"));

        for (final Pair<String, String> test : tests) {
            final String input = "\"\\" + test.getLeft() + "\"";
            final String ref = test.getRight();

            assertEquals(string(cols(0, input.length() - 1), ref), parseExpression(input));
        }
    }

    @Test
    public void testStringUnicodeEscape() {
        for (char c = 0; c < Character.MAX_VALUE; c++) {
            final String input = "\"" + String.format("\\u%04x", (int) c) + "\"";

            assertEquals(string(cols(0, input.length() - 1), Character.toString(c)),
                parseExpression(input));
        }
    }

    @Test
    public void testList() {
        final Expression ref = list(cols(0, 8), integer(cols(1, 1), 1), integer(cols(4, 4), 2),
            integer(cols(7, 7), 3));

        assertEquals(ref, parseExpression("[1, 2, 3]"));
        assertEquals(ref, parseExpression("{1, 2, 3}"));
    }

    @Test
    public void testFunction1() {
        assertEquals(function(cols(0, 11), "average", duration(cols(8, 10), TimeUnit.HOURS, 30)),
            parseFunction("average(30H)"));

        assertEquals(function(cols(0, 7), "sum", duration(cols(4, 6), TimeUnit.HOURS, 30)),
            parseFunction("sum(30H)"));
    }

    @Test
    public void testFunction2a() {
        final FunctionExpression chain = function(cols(0, 43), "chain",
            function(cols(6, 32), "group", list(cols(12, 17), string(cols(13, 16), "host")),
                function(cols(20, 31), "average", duration(cols(28, 30), TimeUnit.HOURS, 30))),
            function(cols(35, 42), "sum", duration(cols(39, 41), TimeUnit.HOURS, 30)));

        assertEquals(chain, parseFunction("chain(group([host], average(30H)), sum(30H))"));
    }

    @Test
    public void testFunction2b() {
        final FunctionExpression chain = function(cols(0, 30), "chain",
            function(cols(0, 19), "group", string(cols(16, 19), "host"),
                function(cols(0, 11), "average", duration(cols(8, 10), TimeUnit.HOURS, 30))),
            function(cols(23, 30), "sum", duration(cols(27, 29), TimeUnit.HOURS, 30)));

        assertEquals(chain, parseFunction("average(30H) by host | sum(30H)"));
    }

    @Test
    public void testFunction3() {
        final FunctionExpression arg1 = function(cols(0, 14), "group", string(cols(11, 14), "host"),
            function(cols(0, 6), "average"));

        final FunctionExpression arg2 =
            function(cols(18, 28), "group", string(cols(25, 28), "site"),
                function(cols(18, 20), "sum"));

        assertEquals(function(cols(0, 28), "chain", arg1, arg2),
            parseFunction("average by host | sum by site"));
    }

    @Test
    public void testFunction4() {
        final FunctionExpression parsed = parseFunction("(average by host | sum) by site");

        final Expression arg1 = string(cols(27, 30), "site");

        final Expression arg2 = function(cols(1, 21), "chain",
            function(cols(1, 15), "group", string(cols(12, 15), "host"),
                function(cols(1, 7), "average")), string(cols(19, 21), "sum"));

        // test grouping
        assertEquals(function(cols(0, 30), "group", arg1, arg2), parsed);
    }

    @Test
    public void testByAll() {
        final FunctionExpression reference =
            function(cols(0, 11), "group", empty(cols(0, 11)), function(cols(0, 6), "average"));
        assertEquals(reference, parseExpression("average by *"));
    }

    @Test
    public void testFilter() {
        final MatchTagFilter f1 = MatchTagFilter.create("a", "a");
        final MatchTagFilter f2 = MatchTagFilter.create("a", "b");
        final MatchTagFilter f3 = MatchTagFilter.create("a", "c");
        final MatchTagFilter f4 = MatchTagFilter.create("b", "b");

        assertEquals(f1, parseFilter("a = a"));
        assertEquals(OrFilter.of(f1, f2, f3), parseFilter("a in [a, b, c]"));
        assertEquals(AndFilter.of(f1, f4), parseFilter("a = a and b = b"));
        assertEquals(OrFilter.of(f1, f4), parseFilter("a = a or b = b"));
        assertEquals(RegexFilter.create("a", "b"), parseFilter("a ~ b"));
        assertEquals(NotFilter.create(f1), parseFilter("a != a"));
        assertEquals(NotFilter.create(f1), parseFilter("!(a = a)"));
        assertEquals(StartsWithFilter.create("a", "a"), parseFilter("a ^ a"));
        assertEquals(HasTagFilter.create("a"), parseFilter("+a"));
        assertEquals(MatchKeyFilter.create("a"), parseFilter("$key = a"));
        assertEquals(TrueFilter.get(), parseFilter("true"));
        assertEquals(FalseFilter.get(), parseFilter("false"));
    }

    @Test
    public void testUnterminatedString() {
        exception.expect(ParseException.class);
        exception.expectMessage("unterminated string");
        parser.parseExpression("\"open");
    }

    @Test
    public void testInvalidSelect() {
        exception.expect(ParseException.class);
        parser.parseExpression("%1");
    }

    @Test
    public void testInvalidGrammar() {
        exception.expect(ParseException.class);
        exception.expectMessage("unexpected token: ~");
        parser.parseQuery("~ from points");
    }

    @Test
    public void testParseDateTime() {
        final Expression.Scope scope = new DefaultScope(0L);

        final Expression e =
            parser.parseExpression("{2014-01-01 00:00:00.000} + {00:01}").eval(scope);

        final InstantExpression expected =
            new InstantExpression(cols(0, 34), Instant.parse("2014-01-01T00:01:00.000Z"));

        assertEquals(expected, e);
    }

    @Test
    public void testArithmetics() {
        final DefaultScope scope = new DefaultScope(10000);

        // numbers
        assertEquals(3L,
            parseExpression("1 + 2 + 3 - 3").eval(scope).cast(IntegerExpression.class).getValue());

        // two strings
        assertEquals("foobar",
            parseExpression("foo + bar").eval(scope).cast(StringExpression.class).getString());

        // two lists
        assertEquals(list(cols(0, 12), string(cols(1, 3), "foo"), string(cols(9, 11), "bar")),
            parseExpression("[foo] + [bar]").eval(scope).cast(ListExpression.class));

        // durations
        assertEquals(duration(cols(0, 6), TimeUnit.MINUTES, 55),
            parseExpression("1H - 5m").eval(scope));
        assertEquals(duration(cols(0, 6), TimeUnit.HOURS, 7),
            parseExpression("3H + 4H").eval(scope));
        assertEquals(duration(cols(0, 8), TimeUnit.MINUTES, 59),
            parseExpression("119m - 1H").eval(scope));
        assertEquals(duration(cols(0, 17), TimeUnit.MINUTES, 60 * 11),
            parseExpression("1H + 1m - 1m + 10H").eval(scope));
    }

    @Test
    public void testFrom() {
        final DefaultScope scope = new DefaultScope(10000);

        checkFrom(MetricType.POINT, Optional.empty(), parseFrom("from points"));
        checkFrom(MetricType.EVENT, Optional.empty(), parseFrom("from events"));

        final Optional<Expression> r1 =
            Optional.of(range(cols(11, 24), integer(col(12), 0), integer(cols(15, 23), 1000)));

        // absolute
        checkFrom(MetricType.POINT, r1, parseFrom("from points(0, 400 + 600)").eval(scope));

        final Optional<Expression> r2 = Optional.of(
            range(cols(11, 18), integer(cols(11, 18), 9000), integer(cols(11, 18), 10000)));

        // relative
        checkFrom(MetricType.POINT, r2, parseFrom("from points(1000ms)").eval(scope));
    }

    @Test
    public void testLetStatement() {
        final Expression expected =
            let(cols(0, 9), reference(cols(4, 5), "a"), query(col(9)).build());

        assertEquals(ImmutableList.of(expected), parser.parse("let $a = *"));
    }

    Context col(int col) {
        return new Context(0, col, 0, col);
    }

    Context cols(int start, int end) {
        return new Context(0, start, 0, end);
    }

    Expression parseExpression(String input) {
        return parser.parseExpression(input);
    }

    FunctionExpression parseFunction(String input) {
        return parseExpression(input).visit(new Expression.Visitor<FunctionExpression>() {
            @Override
            public FunctionExpression visitFunction(final FunctionExpression e) {
                return e;
            }
        });
    }

    Filter parseFilter(final String input) {
        return parser.parseFilter(input);
    }

    void checkFrom(
        MetricType source, Optional<Expression> range, CoreQueryParser.FromDSL result
    ) {
        assertEquals(source, result.getSource());
        assertEquals(range, result.getRange());
    }

    CoreQueryParser.FromDSL parseFrom(String input) {
        return parser.parseFrom(input);
    }

    static QueryBuilder query(Context ctx) {
        return new QueryBuilder(ctx);
    }

    static class QueryBuilder {
        private final Context context;

        private Optional<Expression> select = Optional.empty();
        private Optional<MetricType> source = Optional.empty();
        private Optional<RangeExpression> range = Optional.empty();
        private Optional<Filter> filter = Optional.empty();
        private Map<String, Expression> with = ImmutableMap.of();
        private Map<String, Expression> as = ImmutableMap.of();

        @java.beans.ConstructorProperties({ "context" })
        public QueryBuilder(final Context context) {
            this.context = context;
        }

        public QueryBuilder select(final Expression select) {
            this.select = Optional.of(select);
            return this;
        }

        public QueryBuilder source(final MetricType source) {
            this.source = Optional.of(source);
            return this;
        }

        public QueryBuilder range(final RangeExpression range) {
            this.range = Optional.of(range);
            return this;
        }

        public QueryBuilder filter(final Filter filter) {
            this.filter = Optional.of(filter);
            return this;
        }

        public QueryBuilder with(final Map<String, Expression> with) {
            this.with = with;
            return this;
        }

        public QueryBuilder as(final Map<String, Expression> as) {
            this.as = as;
            return this;
        }

        public QueryExpression build() {
            return new QueryExpression(context, select, source, range, filter, with, as);
        }
    }
}
