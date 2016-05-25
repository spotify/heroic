package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static com.spotify.heroic.grammar.ExpressionTests.uniFuncTest;
import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;
import static org.junit.Assert.assertEquals;

public class DurationExpressionTest {
    private final DurationExpression dur = new DurationExpression(TimeUnit.SECONDS, 42);

    @Test
    public void castTest() {
        assertEquals(dur, dur.cast(DurationExpression.class));
        assertEquals(Expression.integer(dur.toMilliseconds()), dur.cast(IntegerExpression.class));
    }

    @Test
    public void operationsTest() {
        biFuncTest(a -> new DurationExpression(a, TimeUnit.SECONDS, 21L),
            b -> new DurationExpression(b, TimeUnit.SECONDS, 21L),
            r -> new DurationExpression(r, TimeUnit.SECONDS, 42L), DurationExpression::add);

        biFuncTest(a -> new DurationExpression(a, TimeUnit.SECONDS, 21L),
            b -> new DurationExpression(b, TimeUnit.SECONDS, 11L),
            r -> new DurationExpression(r, TimeUnit.SECONDS, 10L), DurationExpression::sub);

        // find least common unit
        biFuncTest(a -> new DurationExpression(a, TimeUnit.MINUTES, 21L),
            b -> new DurationExpression(b, TimeUnit.SECONDS, 10L),
            r -> new DurationExpression(r, TimeUnit.SECONDS, 1250L), DurationExpression::sub);

        uniFuncTest(a -> new DurationExpression(a, TimeUnit.SECONDS, 10L),
            r -> new DurationExpression(r, TimeUnit.SECONDS, -10L), DurationExpression::negate);
    }

    @Test
    public void visitTest() {
        visitorTest(dur, Expression.Visitor::visitDuration);
    }
}
