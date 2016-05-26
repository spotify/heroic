package com.spotify.heroic.grammar;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MinusExpressionTest extends AbstractExpressionTest<MinusExpression> {
    @Override
    protected MinusExpression build(final Context ctx) {
        return new MinusExpression(ctx, a, b);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, MinusExpression, Void> visitorMethod() {
        return Expression.Visitor::visitMinus;
    }

    @Override
    protected Stream<Consumer<MinusExpression>> accessors() {
        return Stream.of(accessorTest(a, MinusExpression::getLeft),
            accessorTest(b, MinusExpression::getRight));
    }

    @Override
    public void evalTest() {
        super.evalTest();

        verify(a).eval(scope);
        verify(b).eval(scope);
    }
}
