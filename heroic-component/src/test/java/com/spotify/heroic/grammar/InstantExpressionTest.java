package com.spotify.heroic.grammar;

import java.time.Instant;
import java.util.function.BiFunction;

public class InstantExpressionTest extends AbstractExpressionTest<InstantExpression> {
    private final Instant instant = Instant.ofEpochMilli(1000);

    @Override
    protected InstantExpression build(final Context ctx) {
        return new InstantExpression(ctx, instant);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, InstantExpression, Void> visitorMethod() {
        return Expression.Visitor::visitInstant;
    }
}
