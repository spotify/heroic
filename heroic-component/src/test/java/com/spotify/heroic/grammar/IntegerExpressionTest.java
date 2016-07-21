package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static com.spotify.heroic.grammar.ExpressionTests.uniFuncTest;
import static org.junit.Assert.assertEquals;

public class IntegerExpressionTest extends AbstractExpressionTest<IntegerExpression> {
    @Override
    protected IntegerExpression build(final Context ctx) {
        return new IntegerExpression(ctx, 42);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, IntegerExpression, Void> visitorMethod() {
        return Expression.Visitor::visitInteger;
    }

    @Test
    public void testAccessors() {
        assertEquals(42, build().getValue());
    }

    @Test
    public void castTest() {
        final IntegerExpression integer = build();

        assertEquals(new DurationExpression(ctx, TimeUnit.MILLISECONDS, integer.getValue()),
            integer.cast(DurationExpression.class));
        assertEquals(new DoubleExpression(ctx, 42D), integer.cast(DoubleExpression.class));
    }

    @Test
    public void operationsTest() {
        biFuncTest(a -> new IntegerExpression(a, 21), b -> new IntegerExpression(b, 21),
            r -> new IntegerExpression(r, 42), IntegerExpression::add);

        biFuncTest(a -> new IntegerExpression(a, 21), b -> new IntegerExpression(b, 21),
            r -> new IntegerExpression(r, 0), IntegerExpression::sub);

        biFuncTest(a -> new IntegerExpression(a, 20), b -> new IntegerExpression(b, 2),
            r -> new IntegerExpression(r, 10), IntegerExpression::divide);

        biFuncTest(a -> new IntegerExpression(a, 10), b -> new IntegerExpression(b, 20),
            r -> new IntegerExpression(r, 200), IntegerExpression::multiply);

        uniFuncTest(a -> new IntegerExpression(a, 10), b -> new IntegerExpression(b, -10),
            IntegerExpression::negate);
    }
}
