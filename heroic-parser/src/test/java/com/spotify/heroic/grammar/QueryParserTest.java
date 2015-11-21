package com.spotify.heroic.grammar;

import static com.spotify.heroic.grammar.Value.list;
import static com.spotify.heroic.grammar.Value.string;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.CoreQueryParser.FromDSL;
import com.spotify.heroic.metric.MetricType;

@RunWith(MockitoJUnitRunner.class)
public class QueryParserTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private CoreQueryParser parser;
    private FilterFactory filters;
    private AggregationFactory aggregations;

    @Mock
    Filter.MatchTag matchTag;
    @Mock
    Filter.And and;
    @Mock
    Filter.Or or;
    @Mock
    Filter optimized;

    @Before
    public void setupFilters() {
        filters = Mockito.mock(FilterFactory.class);
        aggregations = Mockito.mock(AggregationFactory.class);

        Mockito.when(filters.matchTag(Mockito.any(String.class), Mockito.any(String.class)))
                .thenReturn(matchTag);
        Mockito.when(filters.and(anyFilter(), anyFilter())).thenReturn(and);
        Mockito.when(filters.or(anyFilter(), anyFilter())).thenReturn(or);
        Mockito.when(filters.and((List<Filter>) anyList())).thenReturn(and);
        Mockito.when(filters.or((List<Filter>) anyList())).thenReturn(or);
        Mockito.when(or.optimize()).thenReturn(optimized);
        Mockito.when(and.optimize()).thenReturn(optimized);

        parser = new CoreQueryParser(filters, aggregations);
    }

    @Test
    public void testList() {
        assertEquals(Value.list(Value.number(1), Value.number(2), Value.number(3)),
                expr("[1, 2, 3]"));
        assertEquals(expr("[1, 2, 3]"), expr("{1, 2, 3}"));
    }

    @Test
    public void testAggregation() {
        final Value d = Value.duration(TimeUnit.HOURS, 30);

        assertEquals(a("average", d), aggregation("average(30H)"));
        assertEquals(a("sum", d), aggregation("sum(30H)"));

        final AggregationValue chain = a("chain",
                a("group", Value.list(Value.string("host")), a("average", d)), a("sum", d));

        assertEquals(chain, aggregation("chain(group([host], average(30H)), sum(30H))"));
        assertEquals(chain, aggregation("average(30H) by host | sum(30H)"));

        // @formatter:off
        assertEquals(
                a("chain",
                    a("group", list(string("host")), a("average")),
                    a("group", list(string("site")), a("sum"))),
                aggregation("average() by host | sum() by site"));
        // @formatter:on

        // test grouping
        // @formatter:off
        assertEquals(
                a("group",
                    list(string("site")),
                    a("chain",
                        a("group",list(string("host")), a("average")),
                        a("sum"))),
                aggregation("(average() by host | sum()) by site"));
        // @formatter:on
    }

    @Test
    public void testByAll() {
        final AggregationValue reference =
                Value.aggregation("group", Value.list(Value.empty(), Value.aggregation("average")));
        assertEquals(reference, parser.parse(CoreQueryParser.AGGREGATION, "average by *"));
    }

    @Test
    public void testArithmetics() {
        final Value foo = expr("foo"), bar = expr("bar");

        // numbers
        assertEquals((Long) 3L, expr("1 + 2 + 3 - 3").cast(Long.class));

        // two strings
        assertEquals("foobar", expr("foo + bar").cast(String.class));

        // two lists
        assertEquals(Value.list(foo, bar), expr("[foo] + [bar]").cast(ListValue.class));

        // durations
        assertEquals(Value.duration(TimeUnit.MINUTES, 55), expr("1H - 5m"));
        assertEquals(Value.duration(TimeUnit.HOURS, 7), expr("3H + 4H"));
        assertEquals(Value.duration(TimeUnit.MINUTES, 59), expr("119m - 1H"));
        assertEquals(Value.duration(TimeUnit.MINUTES, 60 * 11), expr("1H + 1m - 1m + 10H"));
    }

    @Test
    public void testFrom() {
        checkFrom(MetricType.POINT, Optional.empty(), from("from points"));
        checkFrom(MetricType.EVENT, Optional.empty(), from("from events"));

        // absolute
        checkFrom(MetricType.POINT, Optional.of(new QueryDateRange.Absolute(0, 1234 + 4321)),
                from("from points(0, 1234 + 4321)"));

        // relative
        checkFrom(MetricType.POINT,
                Optional.of(new QueryDateRange.Relative(TimeUnit.MILLISECONDS, 1000)),
                from("from points(1000ms)"));
    }

    @Test
    public void testFilter1() {
        assertEquals(optimized,
                parser.parse(CoreQueryParser.FILTER, "a=b and c=d and d in {foo, bar}"));

        Mockito.verify(filters, Mockito.times(4)).matchTag(Mockito.any(String.class),
                Mockito.any(String.class));
        Mockito.verify(filters, Mockito.times(2)).and(anyFilter(), anyFilter());
        Mockito.verify(and).optimize();
    }

    @Test
    public void testFilter2() {
        assertEquals(optimized,
                parser.parse(CoreQueryParser.FILTER, "a=b and c=d and d in [foo, bar]"));

        Mockito.verify(filters, Mockito.times(4)).matchTag(Mockito.any(String.class),
                Mockito.any(String.class));
        Mockito.verify(filters, Mockito.times(2)).and(anyFilter(), anyFilter());
        Mockito.verify(and).optimize();
    }

    @Test
    public void testUnterminatedString() {
        exception.expect(ParseException.class);
        exception.expectMessage("unterminated string");
        parser.parse(CoreQueryParser.EXPRESSION, "\"open");
    }

    @Test
    public void testErrorChar() {
        exception.expect(ParseException.class);
        exception.expectMessage("garbage");
        parser.parse(CoreQueryParser.SELECT, "* 1");
    }

    @Test
    public void testInvalidSelect() {
        exception.expect(ParseException.class);
        parser.parse(CoreQueryParser.SELECT, "1");
    }

    @Test
    public void testInvalidGrammar() {
        exception.expect(ParseException.class);
        exception.expectMessage("unexpected token: ~");
        parser.parse(CoreQueryParser.QUERY, "~ from points");
    }

    public static AggregationValue a(final String name, final Value... values) {
        return Value.aggregation(name,
                new ListValue(ImmutableList.copyOf(values), Context.empty()));
    }

    void checkFrom(MetricType source, Optional<QueryDateRange> range,
            CoreQueryParser.FromDSL result) {
        assertEquals(source, result.getSource());
        assertEquals(range, result.getRange());
    }

    private AggregationValue aggregation(String input) {
        return parser.parse(CoreQueryParser.AGGREGATION, input);
    }

    static Filter anyFilter() {
        return any(Filter.class);
    }

    Value expr(String input) {
        return parser.parse(CoreQueryParser.EXPRESSION, input);
    }

    FromDSL from(String input) {
        return parser.parse(CoreQueryParser.FROM, input);
    }
}
