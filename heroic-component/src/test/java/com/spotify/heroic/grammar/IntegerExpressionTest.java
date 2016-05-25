package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static org.junit.Assert.assertEquals;

public class IntegerExpressionTest {
    private final IntegerExpression integer = Expression.integer(42);

    @Test
    public void castTest() {
        assertEquals(integer, integer.cast(IntegerExpression.class));
        assertEquals(Expression.duration(TimeUnit.MILLISECONDS, integer.getValue()),
            integer.cast(DurationExpression.class));
        assertEquals(new DoubleExpression(42D), integer.cast(DoubleExpression.class));
    }

    @Test
    public void testOps() {
        biFuncTest(a -> new IntegerExpression(a, 21), b -> new IntegerExpression(b, 21),
            r -> new IntegerExpression(r, 42), IntegerExpression::add);

        biFuncTest(a -> new IntegerExpression(a, 21), b -> new IntegerExpression(b, 21),
            r -> new IntegerExpression(r, 0), IntegerExpression::sub);

        biFuncTest(a -> new IntegerExpression(a, 20), b -> new IntegerExpression(b, 2),
            r -> new IntegerExpression(r, 10), IntegerExpression::divide);

        biFuncTest(a -> new IntegerExpression(a, 10), b -> new IntegerExpression(b, 20),
            r -> new IntegerExpression(r, 200), IntegerExpression::multiply);
    }
}
