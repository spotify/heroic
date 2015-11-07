package com.spotify.heroic.grammar;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;

@RunWith(MockitoJUnitRunner.class)
public class QueryParserTest {
    private CoreQueryParser parser;
    private FilterFactory filters;

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

        Mockito.when(filters.matchTag(Mockito.any(String.class), Mockito.any(String.class))).thenReturn(matchTag);
        Mockito.when(filters.and(anyFilter(), anyFilter())).thenReturn(and);
        Mockito.when(filters.or(Mockito.any(List.class))).thenReturn(or);
        Mockito.when(or.optimize()).thenReturn(optimized);
        Mockito.when(and.optimize()).thenReturn(optimized);

        parser = new CoreQueryParser(filters);
    }

    @Test
    public void testSelect() {
        final AggregationValue average = new AggregationValue("average", ImmutableList.<Value> of(new DiffValue(
                TimeUnit.HOURS, 30)), ImmutableMap.<String, Value> of());

        final AggregationValue sum = new AggregationValue("sum", ImmutableList.<Value> of(new DiffValue(TimeUnit.HOURS,
                30)), ImmutableMap.<String, Value> of());

        final AggregationValue group = new AggregationValue("group", ImmutableList.<Value> of(new ListValue(
                ImmutableList.<Value> of(new StringValue("host"))), average), ImmutableMap.<String, Value> of());

        final AggregationValue chain = new AggregationValue("chain", ImmutableList.<Value> of(group, sum),
                ImmutableMap.<String, Value> of());

        assertEquals(Optional.empty(), parser.parse(CoreQueryParser.SELECT, "*").getAggregation());
        assertEquals(Optional.of(chain),
                parser.parse(CoreQueryParser.SELECT, "chain(group([host], average(30H)), sum(30H))")
                .getAggregation());
    }

    @Test(expected = ParseException.class)
    public void testInvalidSelect() {
        parser.parse(CoreQueryParser.SELECT, "1");
    }

    @Test
    public void testValueExpr() {
        assertEquals("foobar", parser.parse(CoreQueryParser.VALUE_EXPR, "foo + bar").cast(String.class));
        assertEquals(new DiffValue(TimeUnit.HOURS, 7),
                parser.parse(CoreQueryParser.VALUE_EXPR, "3H + 4H").cast(DiffValue.class));
        assertEquals(new DiffValue(TimeUnit.MINUTES, 59),
                parser.parse(CoreQueryParser.VALUE_EXPR, "1H - 1m")
                .cast(DiffValue.class));
        assertEquals(new DiffValue(TimeUnit.MINUTES, 59),
                parser.parse(CoreQueryParser.VALUE_EXPR, "119m - 1H")
                .cast(DiffValue.class));
        assertEquals(new ListValue(ImmutableList.<Value> of(new IntValue(1l), new IntValue(2l))),
                parser.parse(CoreQueryParser.VALUE_EXPR, "[1] + [2]").cast(ListValue.class));
    }

    @Test
    public void testFrom() {
        checkFrom("series", Optional.empty(), parser.parse(CoreQueryParser.FROM, "series"));
        checkFrom("events", Optional.empty(), parser.parse(CoreQueryParser.FROM, "events"));
        // absolute
        checkFrom("series", Optional.of(new DateRange(0, 1234 + 4321)),
                parser.parse(CoreQueryParser.FROM, "series(0, 1234 + 4321)"));
        // relative
        checkFrom("series", Optional.of(new DateRange(0, 1000)),
                parser.parse(CoreQueryParser.FROM, "series(1000ms)", 1000));
    }

    void checkFrom(String source, Optional<DateRange> range, FromDSL result) {
        assertEquals(source, result.getSource());
        assertEquals(range, result.getRange());
    }

    @Test(expected = ParseException.class)
    public void testInvalidGrammar() {
        parser.parse(CoreQueryParser.QUERY, "select ~ from series");
    }

    @Test(expected = ParseException.class)
    public void testInvalidSyntax() {
        parser.parse(CoreQueryParser.QUERY, "select 12 from series");
    }

    @Test
    public void testFilter1() {
        assertEquals(optimized, parser.parse(CoreQueryParser.FILTER, "a=b and c=d and d in {foo, bar}"));

        Mockito.verify(filters, Mockito.times(4)).matchTag(Mockito.any(String.class), Mockito.any(String.class));
        Mockito.verify(filters, Mockito.times(2)).and(anyFilter(), anyFilter());
        Mockito.verify(and).optimize();
    }

    @Test
    public void testFilter2() {
        assertEquals(optimized, parser.parse(CoreQueryParser.FILTER, "a=b and c=d and d in [foo, bar]"));

        Mockito.verify(filters, Mockito.times(4)).matchTag(Mockito.any(String.class), Mockito.any(String.class));
        Mockito.verify(filters, Mockito.times(2)).and(anyFilter(), anyFilter());
        Mockito.verify(and).optimize();
    }

    private Filter anyFilter() {
        return Mockito.any(Filter.class);
    }
}