package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ListExpressionTest extends AbstractExpressionTest<ListExpression> {
    @Override
    protected ListExpression build(final Context ctx) {
        return new ListExpression(ctx, ImmutableList.of(a, b));
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, ListExpression, Void> visitorMethod() {
        return Expression.Visitor::visitList;
    }

    @Override
    protected Stream<Consumer<ListExpression>> accessors() {
        return Stream.of(accessorTest(ImmutableList.of(a, b), ListExpression::getList));
    }

    @Test
    public void operationsTest() {
        biFuncTest(ca -> new ListExpression(ca, ImmutableList.of(a)),
            cb -> new ListExpression(cb, ImmutableList.of(b)),
            cr -> new ListExpression(cr, ImmutableList.of(a, b)), ListExpression::add);
    }

    @Override
    public void evalTest() {
        super.evalTest();

        verify(a).eval(scope);
        verify(b).eval(scope);
    }
}
