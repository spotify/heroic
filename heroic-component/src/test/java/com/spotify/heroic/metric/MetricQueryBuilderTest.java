package com.spotify.heroic.metric;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;

public class MetricQueryBuilderTest {
    private AggregationFactory aggregations;
    private FilterFactory filters;
    private QueryParser parser;
    private Aggregation aggregation;

    private MetricQueryBuilder builder;

    private final DateRange NON_EMPTY = new DateRange(1000, 2000);
    private final DateRange EMPTY = new DateRange(2000, 2000);
    private final Sampling SAMPLING = new Sampling(2000, 100);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        builder = new MetricQueryBuilder(aggregations, filters, parser);
        aggregations = Mockito.mock(AggregationFactory.class);
        filters = Mockito.mock(FilterFactory.class);
        parser = Mockito.mock(QueryParser.class);
        aggregation = mockAggregation();
    }

    private Aggregation mockAggregation() {
        final Aggregation aggregation = Mockito.mock(Aggregation.class);
        Mockito.when(aggregation.sampling()).thenReturn(SAMPLING);
        return aggregation;
    }

    @Test
    public void testValid() {
        assertEquals(new MetricQuery(null, null, null, NON_EMPTY, null, false, DataPoint.class),
                builder.range(NON_EMPTY).build());
    }

    @Test
    public void testAggregation() {
        assertEquals(
                new MetricQuery(null, null, null, NON_EMPTY.rounded(SAMPLING.getSize()).shiftStart(
                        -SAMPLING.getExtent()), aggregation, false, DataPoint.class), builder.range(NON_EMPTY)
                        .aggregation(aggregation).build());
    }

    @Test
    public void testCheckRangeDuringBuild() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("range must not be null");
        builder.build();
    }

    @Test
    public void testAssertRangeSetter() {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("null");
        builder.range(null).build();
    }

    @Test
    public void testAssertNotEmptyRangeSetter() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("range must not be empty");
        builder.range(EMPTY).build();
    }
}