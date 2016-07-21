package com.spotify.heroic.grammar;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.LocalDateTime;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class DateTimeExpressionTest extends AbstractExpressionTest<DateTimeExpression> {
    private final LocalDateTime localDate = LocalDateTime.of(2000, 1, 1, 0, 0, 0, 0);
    private final String dateString = "2000-01-01 00:00:00.000";

    @Override
    protected DateTimeExpression build(final Context ctx) {
        return new DateTimeExpression(ctx, localDate);
    }

    @Override
    protected BiFunction<Expression.Visitor<Void>, DateTimeExpression, Void> visitorMethod() {
        return Expression.Visitor::visitDateTime;
    }

    @Test
    public void parseLocalDateTimeTest() {
        assertEquals(localDate, DateTimeExpression.parseLocalDateTime(dateString));
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseLocalDateTimeThrowsTest() {
        DateTimeExpression.parseLocalDateTime("not a date");
    }

    @Test
    public void parseTest() {
        assertEquals(new DateTimeExpression(ctx, localDate),
            DateTimeExpression.parse(ctx, dateString));
    }

    @Test
    public void toReprTest() {
        assertEquals("{2000-01-01 00:00:00.000}", build().toRepr());
    }
}
