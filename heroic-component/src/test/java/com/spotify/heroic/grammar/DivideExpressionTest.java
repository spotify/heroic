package com.spotify.heroic.grammar;

import static org.mockito.Mockito.verify;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DivideExpressionTest extends AbstractExpressionTest<DivideExpression> {
    @Override
    protected DivideExpression build(final Context ctx) {
        return new DivideExpression(ctx, a, b);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, DivideExpression, Void> visitorMethod() {
        return Expression.Visitor::visitDivide;
    }

    @Override
    protected Stream<Consumer<DivideExpression>> accessors() {
        return Stream.of(accessorTest(a, DivideExpression::getLeft),
            accessorTest(b, DivideExpression::getRight));
    }

    @Override
    public void evalTest() {
        super.evalTest();

        verify(a).eval(scope);
        verify(b).eval(scope);
    }
}
