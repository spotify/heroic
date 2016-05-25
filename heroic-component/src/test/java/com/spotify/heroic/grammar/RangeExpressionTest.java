package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class RangeExpressionTest {
    @Mock
    Expression start;

    @Mock
    Expression end;

    private RangeExpression e;

    @Before
    public void setup() {
        e = new RangeExpression(start, end);
    }

    @Test
    public void testAccessors() {
        assertEquals(start, e.getStart());
        assertEquals(end, e.getEnd());
    }

    @Test
    public void castTest() {
        assertEquals(e, e.cast(RangeExpression.class));
    }

    @Test
    public void visitTest() {
        visitorTest(e, Expression.Visitor::visitRange);
    }

    @Test
    public void toStringTest() {
        doReturn("start").when(start).toString();
        doReturn("end").when(end).toString();
        assertEquals("start -> end", e.toString());
    }
}
