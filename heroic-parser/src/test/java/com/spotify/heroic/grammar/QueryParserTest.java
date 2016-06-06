package com.spotify.heroic.grammar;

import com.spotify.heroic.aggregation.AggregationFactory;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.grammar.Expression.duration;
import static com.spotify.heroic.grammar.Expression.empty;
import static com.spotify.heroic.grammar.Expression.function;
import static com.spotify.heroic.grammar.Expression.integer;
import static com.spotify.heroic.grammar.Expression.list;
import static com.spotify.heroic.grammar.Expression.range;
import static com.spotify.heroic.grammar.Expression.string;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class QueryParserTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private CoreQueryParser parser;
    private AggregationFactory aggregations;

    Expression.Scope scope;

    @Before
    public void setupFilters() {
        aggregations = Mockito.mock(AggregationFactory.class);
        parser = new CoreQueryParser();
        scope = new DefaultScope(10000);
    }

    @Test
    public void testString() {
        assertEquals(string(ctx(0, 2), "foo"), parseExpression("foo"));
        assertEquals(string(ctx(0, 4), "foo"), parseExpression("\"foo\""));
        assertEquals(string(ctx(0, 2), "\u4040"), parseExpression("\"\u4040\""));

        // test all possible escape sequences
        assertEquals(string(ctx(0, 17), "\b\t\n\f\r\"\'\\"),
            parseExpression("\"\\b\\t\\n\\f\\r\\\"\\'\\\\\""));
    }

    @Test
    public void testStringEscapes() {
        final String escapes = "btnfr\"'\\";
        final String references = "\b\t\n\f\r\"\'\\";

        for (int i = 0; i < escapes.length(); i++) {
            final char c = escapes.charAt(i);
            final String ref = Character.toString(references.charAt(i));

            final String input = "\"\\" + Character.toString(c) + "\"";
            final StringExpression s = string(ctx(0, input.length() - 1), ref);

            assertEquals(s, parseExpression(input));
        }
    }

    @Test
    public void testStringUnicodeEscape() {
        for (char c = 0; c < Character.MAX_VALUE; c++) {
            final String input = "\"" + String.format("\\u%04x", (int) c) + "\"";

            assertEquals(string(ctx(0, input.length() - 1), Character.toString(c)),
                parseExpression(input));
        }
    }

    @Test
    public void testList() {
        final Expression ref =
            list(ctx(0, 8), integer(ctx(1, 1), 1), integer(ctx(4, 4), 2), integer(ctx(7, 7), 3));

        assertEquals(ref, parseExpression("[1, 2, 3]"));
        assertEquals(ref, parseExpression("{1, 2, 3}"));
    }

    @Test
    public void testFunction1() {
        assertEquals(function(ctx(0, 11), "average", duration(ctx(8, 10), TimeUnit.HOURS, 30)),
            parseFunction("average(30H)"));

        assertEquals(function(ctx(0, 7), "sum", duration(ctx(4, 6), TimeUnit.HOURS, 30)),
            parseFunction("sum(30H)"));
    }

    @Test
    public void testFunction2a() {
        final FunctionExpression chain = function(ctx(0, 43), "chain",
            function(ctx(6, 32), "group", list(ctx(12, 17), string(ctx(13, 16), "host")),
                function(ctx(20, 31), "average", duration(ctx(28, 30), TimeUnit.HOURS, 30))),
            function(ctx(35, 42), "sum", duration(ctx(39, 41), TimeUnit.HOURS, 30)));

        assertEquals(chain, parseFunction("chain(group([host], average(30H)), sum(30H))"));
    }

    @Test
    public void testFunction2b() {
        final FunctionExpression chain = function(ctx(0, 30), "chain",
            function(ctx(0, 19), "group", string(ctx(16, 19), "host"),
                function(ctx(0, 11), "average", duration(ctx(8, 10), TimeUnit.HOURS, 30))),
            function(ctx(23, 30), "sum", duration(ctx(27, 29), TimeUnit.HOURS, 30)));

        assertEquals(chain, parseFunction("average(30H) by host | sum(30H)"));
    }

    @Test
    public void testFunction3() {
        final FunctionExpression arg1 = function(ctx(0, 14), "group", string(ctx(11, 14), "host"),
            function(ctx(0, 6), "average"));

        final FunctionExpression arg2 = function(ctx(18, 28), "group", string(ctx(25, 28), "site"),
            function(ctx(18, 20), "sum"));

        assertEquals(function(ctx(0, 28), "chain", arg1, arg2),
            parseFunction("average by host | sum by site"));
    }

    @Test
    public void testFunction4() {
        final FunctionExpression parsed = parseFunction("(average by host | sum) by site");

        final Expression arg1 = string(ctx(27, 30), "site");

        final Expression arg2 = function(ctx(1, 21), "chain",
            function(ctx(1, 15), "group", string(ctx(12, 15), "host"),
                function(ctx(1, 7), "average")), string(ctx(19, 21), "sum"));

        // test grouping
        assertEquals(function(ctx(0, 30), "group", arg1, arg2), parsed);
    }

    @Test
    public void testByAll() {
        final FunctionExpression reference =
            function(ctx(0, 11), "group", empty(ctx(0, 11)), function(ctx(0, 6), "average"));
        assertEquals(reference, parseExpression("average by *"));
    }

    @Test
    public void testFilter() {
        final MatchTagFilter f1 = new MatchTagFilter("a", "a");
        final MatchTagFilter f2 = new MatchTagFilter("a", "b");
        final MatchTagFilter f3 = new MatchTagFilter("a", "c");
        final MatchTagFilter f4 = new MatchTagFilter("b", "b");

        assertEquals(f1, parseFilter("a = a"));
        assertEquals(OrFilter.of(f1, f2, f3), parseFilter("a in [a, b, c]"));
        assertEquals(AndFilter.of(f1, f4), parseFilter("a = a and b = b"));
        assertEquals(OrFilter.of(f1, f4), parseFilter("a = a or b = b"));
        assertEquals(new RegexFilter("a", "b"), parseFilter("a ~ b"));
        assertEquals(new NotFilter(f1), parseFilter("a != a"));
        assertEquals(new NotFilter(f1), parseFilter("!(a = a)"));
        assertEquals(new StartsWithFilter("a", "a"), parseFilter("a ^ a"));
        assertEquals(new HasTagFilter("a"), parseFilter("+a"));
        assertEquals(new MatchKeyFilter("a"), parseFilter("$key = a"));
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
            new InstantExpression(ctx(0, 34), Instant.parse("2014-01-01T00:01:00.000Z"));

        assertEquals(expected, e);
    }

    @Test
    public void testArithmetics() {
        // numbers
        assertEquals(3L,
            parseExpression("1 + 2 + 3 - 3").eval(scope).cast(IntegerExpression.class).getValue());

        // two strings
        assertEquals("foobar",
            parseExpression("foo + bar").eval(scope).cast(StringExpression.class).getString());

        // two lists
        assertEquals(list(ctx(0, 12), string(ctx(1, 3), "foo"), string(ctx(9, 11), "bar")),
            parseExpression("[foo] + [bar]").eval(scope).cast(ListExpression.class));

        // durations
        assertEquals(duration(ctx(0, 6), TimeUnit.MINUTES, 55),
            parseExpression("1H - 5m").eval(scope));
        assertEquals(duration(ctx(0, 6), TimeUnit.HOURS, 7),
            parseExpression("3H + 4H").eval(scope));
        assertEquals(duration(ctx(0, 8), TimeUnit.MINUTES, 59),
            parseExpression("119m - 1H").eval(scope));
        assertEquals(duration(ctx(0, 17), TimeUnit.MINUTES, 60 * 11),
            parseExpression("1H + 1m - 1m + 10H").eval(scope));
    }

    @Test
    public void testFrom() {
        checkFrom(MetricType.POINT, Optional.empty(), parseFrom("from points"));
        checkFrom(MetricType.EVENT, Optional.empty(), parseFrom("from events"));

        final Optional<Expression> r1 =
            Optional.of(range(ctx(11, 24), integer(ctx(12, 12), 0), integer(ctx(15, 23), 1000)));

        // absolute
        checkFrom(MetricType.POINT, r1, parseFrom("from points(0, 400 + 600)").eval(scope));

        final Optional<Expression> r2 = Optional.of(
            range(ctx(11, 18), integer(ctx(11, 18), 9000), integer(ctx(11, 18), 10000)));

        // relative
        checkFrom(MetricType.POINT, r2, parseFrom("from points(1000ms)").eval(scope));
    }

    Context ctx(int start, int end) {
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

    /*
    TO BE PORTED TESTS:

    @Test
    public void testMultipleStatements() {
        final StringBuilder query = new StringBuilder();
        query.append("let $a = * from points(1d);\n");
        query.append("let $b = * from points($now - 2d, $now - 1d);\n");
        query.append("$a + $b as key = \"value\";");

        // @formatter:off
        final List<Expression> expected = new ArrayList<>();

        expected.add(let(reference("a"),
                query(empty(), of(MetricType.POINT),
                    of(range(
                        minus(reference("now"),
                            duration(TimeUnit.DAYS, 1)),
                        reference("now"))), empty(), ImmutableMap.of(), ImmutableMap.of())));
        expected.add(
            let(reference("b"),
                query(empty(), of(MetricType.POINT),
                    of(range(
                        minus(reference("now"), duration(TimeUnit.DAYS, 2)),
                        minus(reference("now"), duration(TimeUnit.DAYS, 1)))),
                    empty(), ImmutableMap.of(), ImmutableMap.of())));

        expected.add(
            query(of(plus(reference("a"), reference("b"))), empty(), empty(), empty(),
                ImmutableMap.of(), ImmutableMap.of("key", string("value"))));
        // @formatter:on

        assertEquals(expected, parser.parse(query.toString()));
    }
    */
}
