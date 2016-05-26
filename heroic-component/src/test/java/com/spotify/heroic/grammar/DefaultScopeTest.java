package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultScopeTest {
    private DefaultScope scope;

    @Before
    public void setup() {
        scope = new DefaultScope(42L);
    }

    @Test
    public void lookupTest() {
        assertEquals(Expression.integer(42L), scope.lookup(Expression.NOW));
    }

    @Test(expected = ParseException.class)
    public void lookupMissingTest() {
        scope.lookup("missing");
    }
}
