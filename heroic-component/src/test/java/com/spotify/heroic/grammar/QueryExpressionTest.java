package com.spotify.heroic.grammar;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricType;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryExpressionTest extends AbstractExpressionTest<QueryExpression> {
    final Optional<Expression> select = Optional.empty();
    final Optional<MetricType> source = Optional.empty();
    final Optional<RangeExpression> range = Optional.empty();
    final Optional<Filter> filter = Optional.empty();
    final Map<String, Expression> with = ImmutableMap.of();
    final Map<String, Expression> as = ImmutableMap.of();

    @Override
    protected QueryExpression build(final Context ctx) {
        return new QueryExpression(ctx, select, source, range, filter, with, as);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, QueryExpression, Void> visitorMethod() {
        return Expression.Visitor::visitQuery;
    }

    @Test
    public void testAccessors() {
        final QueryExpression e = build();

        assertEquals(select, e.getSelect());
        assertEquals(source, e.getSource());
        assertEquals(range, e.getRange());
        assertEquals(filter, e.getFilter());
        assertEquals(with, e.getWith());
        assertEquals(as, e.getAsExpression());
    }
}
