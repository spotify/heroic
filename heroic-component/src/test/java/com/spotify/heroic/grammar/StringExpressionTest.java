package com.spotify.heroic.grammar;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

public class StringExpressionTest {
    private final StringExpression str = Expression.string("hello");

    @Test
    public void castTest() {
        assertEquals(str, str.cast(StringExpression.class));
        assertEquals(Expression.list(str), str.cast(ListExpression.class));
        assertEquals(Expression.function(str.getString()), str.cast(FunctionExpression.class));
    }

    @Test
    public void addTest() {
        final Context c = Mockito.mock(Context.class);
        final Context r = Mockito.mock(Context.class);
        final StringExpression a = new StringExpression(c, "a");
        final StringExpression b = new StringExpression(c, "b");

        doReturn(r).when(c).join(c);

        final Expression added = a.add(b);

        assertEquals(Expression.string("ab"), added);
        verify(c).join(c);

        assertEquals(r, added.context());
    }
}
