package com.spotify.heroic.grammar;

import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MultiplyExpressionTest extends AbstractExpressionTest<MultiplyExpression> {
    @Override
    protected MultiplyExpression build(final Context ctx) {
        return new MultiplyExpression(ctx, a, b);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, MultiplyExpression, Void> visitorMethod() {
        return Expression.Visitor::visitMultiply;
    }

    @Override
    protected Stream<Consumer<MultiplyExpression>> accessors() {
        return Stream.of(accessorTest(a, MultiplyExpression::getLeft),
            accessorTest(b, MultiplyExpression::getRight));
    }

    @Override
    public void evalTest() {
        super.evalTest();

        verify(a).eval(scope);
        verify(b).eval(scope);
    }
}
