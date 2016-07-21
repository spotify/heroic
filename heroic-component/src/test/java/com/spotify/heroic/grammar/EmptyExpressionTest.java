package com.spotify.heroic.grammar;

import java.util.function.BiFunction;

public class EmptyExpressionTest extends AbstractExpressionTest<EmptyExpression> {
    @Override
    protected EmptyExpression build(final Context ctx) {
        return new EmptyExpression(ctx);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, EmptyExpression, Void> visitorMethod() {
        return Expression.Visitor::visitEmpty;
    }
}
