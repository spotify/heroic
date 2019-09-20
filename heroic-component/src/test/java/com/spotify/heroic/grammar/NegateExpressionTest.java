package com.spotify.heroic.grammar;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NegateExpressionTest extends AbstractExpressionTest<NegateExpression> {
    @Before
    public void setupLocal() {
        doReturn(a).when(a).negate();
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
