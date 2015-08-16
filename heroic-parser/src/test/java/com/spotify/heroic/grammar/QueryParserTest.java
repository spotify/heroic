package com.spotify.heroic.grammar;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;

public class QueryParserTest {
    private CoreQueryParser parser;
    private FilterFactory filters;

    @Before
    public void setupFilters() {
        filters = Mockito.mock(FilterFactory.class);
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

        assertEquals(Optional.absent(), parser.parse(CoreQueryParser.SELECT, "*").getAggregation());
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
        checkFrom("series", Optional.absent(), parser.parse(CoreQueryParser.FROM, "series"));
        checkFrom("events", Optional.absent(), parser.parse(CoreQueryParser.FROM, "events"));
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
    public void testFilter() {
        final Filter.MatchTag matchTag = Mockito.mock(Filter.MatchTag.class);
        final Filter.And and = Mockito.mock(Filter.And.class);
        final Filter optimized = Mockito.mock(Filter.class);

        Mockito.when(filters.matchTag(Mockito.any(String.class), Mockito.any(String.class))).thenReturn(matchTag);
        Mockito.when(filters.and(matchTag, matchTag)).thenReturn(and);
        Mockito.when(and.optimize()).thenReturn(optimized);

        assertEquals(optimized, parser.parse(CoreQueryParser.FILTER, "a=b and c=d"));

        Mockito.verify(filters, Mockito.times(2)).matchTag(Mockito.any(String.class), Mockito.any(String.class));
        Mockito.verify(filters).and(matchTag, matchTag);
        Mockito.verify(and).optimize();
    }
}