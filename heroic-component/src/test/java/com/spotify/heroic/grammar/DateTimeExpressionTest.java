package com.spotify.heroic.grammar;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.Month;

import static com.spotify.heroic.grammar.ExpressionTests.visitorTest;

public class DateTimeExpressionTest {
    private final DateTimeExpression d =
        new DateTimeExpression(LocalDateTime.of(2000, Month.JANUARY, 1, 0, 0));

    @Test
    public void visitTest() {
        visitorTest(d, Expression.Visitor::visitDateTime);
    }
}
