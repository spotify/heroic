package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static com.spotify.heroic.grammar.ExpressionTests.uniFuncTest;
import static org.junit.Assert.assertEquals;

public class DurationExpressionTest extends AbstractExpressionTest<DurationExpression> {
    @Override
    protected DurationExpression build(final Context ctx) {
        return new DurationExpression(ctx, TimeUnit.SECONDS, 42);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, DurationExpression, Void> visitorMethod() {
        return Expression.Visitor::visitDuration;
    }

    @Test
    public void castTest() {
        final DurationExpression dur = build();

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
}
