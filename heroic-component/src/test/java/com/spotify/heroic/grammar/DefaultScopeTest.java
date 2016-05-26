package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class DefaultScopeTest {
    @Mock
    Context ctx;

    private DefaultScope scope;

    @Before
    public void setup() {
        scope = new DefaultScope(42L);
    }

    @Test
    public void lookupTest() {
        assertEquals(new IntegerExpression(ctx, 42L), scope.lookup(ctx, Expression.NOW));
    }

    @Test(expected = ParseException.class)
    public void lookupMissingTest() {
        doReturn(new ParseException("", null, 0, 0, 0, 0)).when(ctx).scopeLookupError("missing");

        scope.lookup(ctx, "missing");
    }
}
