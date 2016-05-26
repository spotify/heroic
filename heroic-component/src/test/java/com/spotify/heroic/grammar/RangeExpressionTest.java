package com.spotify.heroic.grammar;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class RangeExpressionTest extends AbstractExpressionTest<RangeExpression> {
    @Override
    protected RangeExpression build(final Context ctx) {
        return new RangeExpression(ctx, a, b);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, RangeExpression, Void> visitorMethod() {
        return Expression.Visitor::visitRange;
    }

    @Override
    protected Stream<Consumer<RangeExpression>> accessors() {
        return Stream.of(accessorTest(a, RangeExpression::getStart),
            accessorTest(b, RangeExpression::getEnd));
    }

    @Test
    public void testAccessors() {
        final RangeExpression e = build();

        assertEquals(a, e.getStart());
        assertEquals(b, e.getEnd());
    }

    @Test
    public void toStringTest() {
        final RangeExpression e = build();
        assertEquals("a -> b", e.toString());
    }
}
