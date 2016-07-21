package com.spotify.heroic.grammar;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ReferenceExpressionTest extends AbstractExpressionTest<ReferenceExpression> {
    private final String name = "ref";

    @Override
    protected ReferenceExpression build(final Context ctx) {
        return new ReferenceExpression(ctx, name);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, ReferenceExpression, Void> visitorMethod() {
        return Expression.Visitor::visitReference;
    }

    @Test
    public void testAccessors() {
        final ReferenceExpression e = build();

        assertEquals(name, e.getName());
    }
}
