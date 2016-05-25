package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NegateExpressionTest {
    @Mock
    private Expression expr;

    private NegateExpression e;

    @Before
    public void setup() {
        e = new NegateExpression(expr);
    }

    @Test
    public void castTest() {
        final Expression negated = Mockito.mock(Expression.class);

        doReturn(negated).when(expr).negate();

        e.cast(IntegerExpression.class);

        verify(expr).negate();
        verify(negated).cast(IntegerExpression.class);
    }

    @Test
    public void visitTest() {
        visitorTest(e, Expression.Visitor::visitNegate);
    }
}
