package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableMap;

import java.util.function.BiFunction;

public class FunctionExpressionTest extends AbstractExpressionTest<FunctionExpression> {
    @Override
    protected FunctionExpression build(final Context ctx) {
        return new FunctionExpression(ctx, "foo", Expression.list(), ImmutableMap.of());
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, FunctionExpression, Void> visitorMethod() {
        return Expression.Visitor::visitFunction;
    }
}
