package com.spotify.heroic.grammar;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.grammar.ExpressionTests.biFuncTest;
import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ListExpressionTest {
    @Mock
    Expression a;

    @Mock
    Expression b;

    @Mock
    Expression c;

    private List<Expression> list;
    private ListExpression e;

    @Before
    public void setup() {
        list = ImmutableList.of(a, b, c);
        e = new ListExpression(list);
    }

    @Test
    public void testAccessors() {
        assertEquals(list, e.getList());
    }

    @Test
    public void castTest() {
        assertEquals(e, e.cast(ListExpression.class));
    }

    @Test
    public void operationsTest() {
        biFuncTest(ca -> new ListExpression(ca, ImmutableList.of(a, b)),
            cb -> new ListExpression(cb, ImmutableList.of(c)), cr -> new ListExpression(cr, list),
            ListExpression::add);
    }

    @Test
    public void visitTest() {
        visitorTest(e, Expression.Visitor::visitList);
    }
}
