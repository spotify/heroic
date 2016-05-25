package com.spotify.heroic.grammar;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ContextTest {
    private final Context c1 = new Context(0, 0, 10, 10);
    private final Context c2 = new Context(10, 10, 20, 20);

    @Test
    public void joinTest() {
        assertEquals(new Context(0, 0, 20, 20), c1.join(c2));
    }

    @Test
    public void nameTest() {
        assertEquals("a", Context.name(A.class));
        assertEquals("B", Context.name(B.class));
    }

    @Test
    public void errorTest() {
        final ParseException p = c1.error("message");

        assertEquals("0:0: message", p.getMessage());
        assertEquals(null, p.getCause());
        assertEquals(0, p.getLine());
        assertEquals(0, p.getCol());
        assertEquals(10, p.getLineEnd());
        assertEquals(10, p.getColEnd());
    }

    @Test
    public void errorTestCause() {
        final Exception e = new Exception("message");
        final ParseException p = c1.error(e);

        assertEquals("0:0: " + e.getMessage(), p.getMessage());
        assertEquals(e, p.getCause());
        assertEquals(0, p.getLine());
        assertEquals(0, p.getCol());
        assertEquals(10, p.getLineEnd());
        assertEquals(10, p.getColEnd());
    }

    @Test
    public void castErrorTest() {
        final Object o = new Object() {
            @Override
            public String toString() {
                return "object";
            }
        };

        final ParseException p = c1.castError(o, A.class);

        assertEquals("0:0: object cannot be cast to a", p.getMessage());
        assertEquals(null, p.getCause());
        assertEquals(0, p.getLine());
        assertEquals(0, p.getCol());
        assertEquals(10, p.getLineEnd());
        assertEquals(10, p.getColEnd());
    }

    @Test
    public void scopeLookupErrorTest() {
        final ParseException p = c1.scopeLookupError("foo");

        assertEquals("0:0: cannot find reference (foo) in the current scope", p.getMessage());
        assertEquals(null, p.getCause());
        assertEquals(0, p.getLine());
        assertEquals(0, p.getCol());
        assertEquals(10, p.getLineEnd());
        assertEquals(10, p.getColEnd());
    }

    @Test
    public void emptyTest() {
        assertEquals(new Context(-1, -1, -1, -1), Context.empty());
    }

    @JsonTypeName("a")
    interface A {
    }

    interface B {
    }
}
