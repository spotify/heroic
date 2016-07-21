package com.spotify.heroic.grammar;

import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class PlusExpressionTest extends AbstractExpressionTest<PlusExpression> {
    @Override
    protected PlusExpression build(final Context ctx) {
        return new PlusExpression(ctx, a, b);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, PlusExpression, Void> visitorMethod() {
        return Expression.Visitor::visitPlus;
    }

    @Override
    protected Stream<Consumer<PlusExpression>> accessors() {
        return Stream.of(accessorTest(a, PlusExpression::getLeft),
            accessorTest(b, PlusExpression::getRight));
    }

    @Override
    public void evalTest() {
        super.evalTest();

        verify(a).eval(scope);
        verify(b).eval(scope);
    }
}
