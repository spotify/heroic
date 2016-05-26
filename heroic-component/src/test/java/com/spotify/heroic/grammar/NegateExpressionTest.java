package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NegateExpressionTest extends AbstractExpressionTest<NegateExpression> {
    @Before
    public void setupLocal() {
        doReturn(a).when(a).negate();
        doReturn(b).when(b).negate();
    }

    @Override
    protected NegateExpression build(final Context ctx) {
        return new NegateExpression(ctx, a);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, NegateExpression, Void> visitorMethod() {
        return Expression.Visitor::visitNegate;
    }

    @Override
    public void evalTest() {
        super.evalTest();
        verify(a).eval(scope);
    }
}
