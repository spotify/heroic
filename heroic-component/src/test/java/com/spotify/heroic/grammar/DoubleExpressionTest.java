package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static org.junit.Assert.assertEquals;

public class DoubleExpressionTest {
    private final DoubleExpression d = new DoubleExpression(42D);

    @Test
    public void castTest() {
        assertEquals(d, d.cast(DoubleExpression.class));
        assertEquals(new IntegerExpression((long) d.getValue()), d.cast(IntegerExpression.class));
        assertEquals(Expression.duration(TimeUnit.MILLISECONDS, (int) d.getValue()),
            d.cast(DurationExpression.class));
    }

    @Test
    public void testOps() {
        biFuncTest(a -> new DoubleExpression(a, 21D), b -> new DoubleExpression(b, 21D),
            r -> new DoubleExpression(r, 42D), DoubleExpression::add);

        biFuncTest(a -> new DoubleExpression(a, 21D), b -> new DoubleExpression(b, 21D),
            r -> new DoubleExpression(r, 0D), DoubleExpression::sub);

        biFuncTest(a -> new DoubleExpression(a, 20D), b -> new DoubleExpression(b, 2D),
            r -> new DoubleExpression(r, 10D), DoubleExpression::divide);

        biFuncTest(a -> new DoubleExpression(a, 10D), b -> new DoubleExpression(b, 20D),
            r -> new DoubleExpression(r, 200D), DoubleExpression::multiply);
    }
}
