package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractExpressionTest<E extends Expression> {
    private final ParseException e = new ParseException("", null, 0, 0, 0, 0);

    @Mock
    protected Context ctx;

    @Mock
    protected Expression.Scope scope;

    @Mock
    protected Expression lookup;

    @Mock
    protected Expression a;

    @Mock
    protected Expression b;

    @Before
    public final void abstractSetup() {
        doReturn(lookup).when(scope).lookup(eq(ctx), anyString());
        doReturn(lookup).when(lookup).eval(scope);

        doReturn(a).when(a).eval(scope);
        doReturn(b).when(b).eval(scope);

        doReturn("a").when(a).toString();
        doReturn("b").when(b).toString();

        doReturn("a").when(a).toRepr();
        doReturn("b").when(b).toRepr();

        doReturn(e).when(ctx).castError(anyObject(), any(Class.class));
    }

    protected abstract E build(Context ctx);

    protected abstract BiFunction<Expression.Visitor<Void>, E, Void> visitorMethod();

    protected E build() {
        return build(ctx);
    }

    protected Stream<Consumer<E>> accessors() {
        return Stream.of();
    }

    protected <V> Consumer<E> accessorTest(final V expected, final Function<E, V> accessor) {
        return e -> {
            assertEquals(expected, accessor.apply(e));
        };
    }

    @Test
    public void getContextTest() {
        final E expr = build();
        assertEquals(ctx, expr.getContext());
    }

    @Test
    public void visitTest() {
        visitorTest(build(), visitorMethod());
    }

    @Test
    public void selfCastTest() {
        final E expr = build();
        assertEquals(expr, expr.cast(expr.getClass()));
    }

    @Test
    public void evalTest() {
        build().eval(scope);
    }

    @Test
    public final void accessorsTest() {
        final E e = build();

        accessors().forEach(accessor -> {
            accessor.accept(e);
        });
    }

    @Test
    public void toStringTest() {
        final String str = build().toString();
        assertFalse(str.isEmpty());
        // assert that default Object#toString has been overriden.
        assertFalse(str.startsWith("java.lang.Object"));
    }

    @Test(expected = ParseException.class)
    public void castErrorTest() {
        build().cast(IllegalExpression.class);
    }

    interface IllegalExpression extends Expression {
    }
}
