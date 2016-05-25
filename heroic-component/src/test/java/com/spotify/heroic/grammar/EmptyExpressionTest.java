package com.spotify.heroic.grammar;

import org.junit.Test;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;

public class EmptyExpressionTest {
    private EmptyExpression e = new EmptyExpression();

    @Test
    public void visitTest() {
        visitorTest(e, Expression.Visitor::visitEmpty);
    }
}
