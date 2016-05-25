package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class LetExpressionTest {
    @Mock
    ReferenceExpression ref;

    @Mock
    Expression expr;

    private LetExpression e;

    @Before
    public void setup() {
        e = new LetExpression(ref, expr);
    }

    @Test
    public void testAccessors() {
        assertEquals(ref, e.getReference());
        assertEquals(expr, e.getExpression());
    }

    @Test
    public void visitTest() {
        visitorTest(e, Expression.Visitor::visitLet);
    }
}
