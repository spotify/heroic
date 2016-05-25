package com.spotify.heroic.grammar;

import org.junit.Test;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;
import static org.junit.Assert.assertEquals;

public class StringExpressionTest {
    private final StringExpression str = Expression.string("hello");

    @Test
    public void castTest() {
        assertEquals(str, str.cast(StringExpression.class));
        assertEquals(Expression.list(str), str.cast(ListExpression.class));
        assertEquals(Expression.function(str.getString()), str.cast(FunctionExpression.class));
    }

    @Test
    public void operationsTest() {
        biFuncTest(a -> new StringExpression(a, "foo"), b -> new StringExpression(b, "bar"),
            r -> new StringExpression(r, "foobar"), StringExpression::add);
    }

    @Test
    public void visitTest() {
        visitorTest(str, Expression.Visitor::visitString);
    }
}
