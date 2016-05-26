package com.spotify.heroic.grammar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class ExpressionTest {
    private final ParseException parseException = new ParseException("", null, 0, 0, 0, 0);

    @Mock
    Context ctx;

    @Mock
    Expression other;

    Expression e;

    @Before
    public void setup() {
        e = Mockito.mock(Expression.class, Mockito.CALLS_REAL_METHODS);
        doReturn(ctx).when(e).getContext();
        doReturn(parseException).when(ctx).error(anyString());
    }

    @Test(expected = ParseException.class)
    public void defaultAddTest() {
        e.add(other);
    }

    @Test(expected = ParseException.class)
    public void defaultSubTest() {
        e.sub(other);
    }

    @Test(expected = ParseException.class)
    public void defaultMultiplyTest() {
        e.multiply(other);
    }

    @Test(expected = ParseException.class)
    public void defaultDivideTest() {
        e.divide(other);
    }

    @Test(expected = ParseException.class)
    public void defaultNegateTest() {
        e.negate();
    }
}
