package com.spotify.heroic.grammar;

import static org.junit.Assert.assertEquals;

import java.util.function.BiFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LetExpressionTest extends AbstractExpressionTest<LetExpression> {
    @Mock
    ReferenceExpression ref;

    @Override
    protected LetExpression build(final Context ctx) {
        return new LetExpression(ctx, ref, a);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, LetExpression, Void> visitorMethod() {
        return Expression.Visitor::visitLet;
    }

    @Test
    public void testAccessors() {
        final LetExpression e = build();

        assertEquals(ref, e.getReference());
        assertEquals(a, e.getExpression());
    }
}
