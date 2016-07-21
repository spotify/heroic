package com.spotify.heroic.grammar;

import org.junit.Test;

import java.util.function.BiFunction;

import static com.spotify.heroic.grammar.Expression.function;
import static com.spotify.heroic.grammar.Expression.list;
import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static org.junit.Assert.assertEquals;

public class StringExpressionTest extends AbstractExpressionTest<StringExpression> {
    private final String value = "hello";

    @Override
    protected StringExpression build(final Context ctx) {
        return new StringExpression(ctx, value);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, StringExpression, Void> visitorMethod() {
        return Expression.Visitor::visitString;
    }

    @Test
    public void castTest() {
        final StringExpression str = build();

        assertEquals(list(ctx, str), str.cast(ListExpression.class));
        assertEquals(function(ctx, str.getString()), str.cast(FunctionExpression.class));
    }

    @Test
    public void operationsTest() {
        biFuncTest(a -> new StringExpression(a, "foo"), b -> new StringExpression(b, "bar"),
            r -> new StringExpression(r, "foobar"), StringExpression::add);
    }
}
