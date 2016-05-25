package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;

public class FunctionExpressionTest {
    private FunctionExpression e =
        new FunctionExpression("foo", Expression.list(), ImmutableMap.of());

    @Test
    public void visitTest() {
        visitorTest(e, Expression.Visitor::visitFunction);
    }
}
