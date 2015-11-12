package com.spotify.heroic.grammar;

import static org.junit.Assert.assertEquals;

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
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
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
        Mockito.when(filters.or(Mockito.any(List.class))).thenReturn(or);
        Mockito.when(or.optimize()).thenReturn(optimized);
        Mockito.when(and.optimize()).thenReturn(optimized);

        parser = new CoreQueryParser(filters, aggregations);
    }

    @Test
    public void testAggregation() {
        final AggregationValue average = new AggregationValue("average",
                ImmutableList.<Value> of(new DurationValue(TimeUnit.HOURS, 30)),
                ImmutableMap.<String, Value> of());

        final AggregationValue sum = new AggregationValue("sum",
                ImmutableList.<Value> of(new DurationValue(TimeUnit.HOURS, 30)),
                ImmutableMap.<String, Value> of());

        final AggregationValue group = new AggregationValue("group",
                ImmutableList.<Value> of(
                        new ListValue(ImmutableList.<Value> of(new StringValue("host"))), average),
                ImmutableMap.<String, Value> of());

        final AggregationValue chain = new AggregationValue("chain",
                ImmutableList.<Value> of(group, sum), ImmutableMap.<String, Value> of());

        assertEquals(chain, parser.parse(CoreQueryParser.AGGREGATION,
                "chain(group([host], average(30H)), sum(30H))"));
    }

    @Test
    public void testValueExpr() {
        assertEquals("foobar",
                parser.parse(CoreQueryParser.VALUE_EXPR, "foo + bar").cast(String.class));
        assertEquals(new DurationValue(TimeUnit.HOURS, 7),
                parser.parse(CoreQueryParser.VALUE_EXPR, "3H + 4H").cast(DurationValue.class));
        assertEquals(new DurationValue(TimeUnit.MINUTES, 59),
                parser.parse(CoreQueryParser.VALUE_EXPR, "1H - 1m").cast(DurationValue.class));
        assertEquals(new DurationValue(TimeUnit.MINUTES, 59),
                parser.parse(CoreQueryParser.VALUE_EXPR, "119m - 1H").cast(DurationValue.class));
        assertEquals(new ListValue(ImmutableList.<Value> of(new IntValue(1L), new IntValue(2L))),
                parser.parse(CoreQueryParser.VALUE_EXPR, "[1] + [2]").cast(ListValue.class));
    }

    @Test
    public void testFrom() {
        checkFrom(MetricType.POINT, Optional.empty(),
                parser.parse(CoreQueryParser.FROM, "from points"));
        checkFrom(MetricType.EVENT, Optional.empty(),
                parser.parse(CoreQueryParser.FROM, "from events"));

        // absolute
        checkFrom(MetricType.POINT, Optional.of(new QueryDateRange.Absolute(0, 1234 + 4321)),
                parser.parse(CoreQueryParser.FROM, "from points(0, 1234 + 4321)"));

        // relative
        checkFrom(MetricType.POINT,
                Optional.of(new QueryDateRange.Relative(TimeUnit.MILLISECONDS, 1000)),
                parser.parse(CoreQueryParser.FROM, "from points(1000ms)", 1000));
    }

    void checkFrom(MetricType source, Optional<QueryDateRange> range,
            CoreQueryParser.FromDSL result) {
        assertEquals(source, result.getSource());
        assertEquals(range, result.getRange());
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
        parser.parse(CoreQueryParser.VALUE_EXPR, "\"open");
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
        exception.expectMessage("expected <aggregation>, but was <1>");
        parser.parse(CoreQueryParser.SELECT, "1");
    }

    @Test
    public void testInvalidGrammar() {
        exception.expect(ParseException.class);
        exception.expectMessage("unexpected token: ~");
        parser.parse(CoreQueryParser.QUERY, "~ from points");
    }

    private Filter anyFilter() {
        return Mockito.any(Filter.class);
    }
}
