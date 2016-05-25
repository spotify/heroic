package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;

@RunWith(MockitoJUnitRunner.class)
public class MinusExpressionTest {
    @Mock
    private Expression a;

    @Mock
    private Expression b;

    private MinusExpression e;

    @Before
    public void setup() {
        e = new MinusExpression(a, b);
    }

    @Test
    public void visitTest() {
        visitorTest(e, Expression.Visitor::visitMinus);
    }
}
