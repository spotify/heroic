package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class TimeExpressionTest extends AbstractExpressionTest<TimeExpression> {
    @Override
    protected TimeExpression build(final Context ctx) {
        return new TimeExpression(ctx, 0, 0, 0, 0);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, TimeExpression, Void> visitorMethod() {
        return Expression.Visitor::visitTime;
    }

    @Override
    public void evalTest() {
        // do nothing
    }

    @Test
    public void parseTest() {
        assertEquals(build(), TimeExpression.parse(ctx, "00:00:00.000"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseErrorTest() {
        TimeExpression.parse(ctx, "not a time");
    }
}
