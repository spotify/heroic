package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static com.spotify.heroic.grammar.ExpressionTests.uniFuncTest;
import static org.junit.Assert.assertEquals;

public class DoubleExpressionTest extends AbstractExpressionTest<DoubleExpression> {
    @Override
    protected DoubleExpression build(final Context ctx) {
        return new DoubleExpression(ctx, 42D);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, DoubleExpression, Void> visitorMethod() {
        return Expression.Visitor::visitDouble;
    }

    @Test
    public void castTest() {
        final DoubleExpression d = build();

        assertEquals(new IntegerExpression(ctx, (long) d.getValue()),
            d.cast(IntegerExpression.class));
        assertEquals(new DurationExpression(ctx, TimeUnit.MILLISECONDS, (int) d.getValue()),
            d.cast(DurationExpression.class));
    }

    @Test
    public void operationsTest() {
        biFuncTest(a -> new DoubleExpression(a, 21D), b -> new DoubleExpression(b, 21D),
            r -> new DoubleExpression(r, 42D), DoubleExpression::add);

        biFuncTest(a -> new DoubleExpression(a, 21D), b -> new DoubleExpression(b, 21D),
            r -> new DoubleExpression(r, 0D), DoubleExpression::sub);

        biFuncTest(a -> new DoubleExpression(a, 20D), b -> new DoubleExpression(b, 2D),
            r -> new DoubleExpression(r, 10D), DoubleExpression::divide);

        biFuncTest(a -> new DoubleExpression(a, 10D), b -> new DoubleExpression(b, 20D),
            r -> new DoubleExpression(r, 200D), DoubleExpression::multiply);

        uniFuncTest(a -> new DoubleExpression(a, 10D), r -> new DoubleExpression(r, -10D),
            DoubleExpression::negate);
    }
}
