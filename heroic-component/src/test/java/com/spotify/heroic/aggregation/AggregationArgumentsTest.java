package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.grammar.Expression;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class AggregationArgumentsTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Mock
    public Expression a;

    @Mock
    public Expression b;

    @Before
    public void setup() {
        doReturn(a).when(a).cast(Expression.class);
        doReturn(b).when(b).cast(Expression.class);
    }

    @After
    public void teardown() {
    }

    @Test
    public void all() {
        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(a, b), ImmutableMap.of());

        final List<Expression> result = arguments.all(Expression.class);

        assertEquals(ImmutableList.of(a, b), result);

        verify(a).cast(Expression.class);
        verify(b).cast(Expression.class);
    }

    @Test
    public void positional() {
        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(a, b), ImmutableMap.of());

        assertEquals(Optional.of(a), arguments.positional(Expression.class));
        assertEquals(Optional.of(b), arguments.positional(Expression.class));
        assertEquals(Optional.empty(), arguments.positional(Expression.class));

        verify(a).cast(Expression.class);
        verify(b).cast(Expression.class);
    }

    @Test
    public void keyword() {
        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(), ImmutableMap.of("a", a, "b", b));

        assertEquals(Optional.of(a), arguments.keyword("a", Expression.class));
        assertEquals(Optional.empty(), arguments.keyword("a", Expression.class));
        assertEquals(Optional.of(b), arguments.keyword("b", Expression.class));

        verify(a).cast(Expression.class);
        verify(b).cast(Expression.class);
    }

    @Test
    public void positionalOrKeyword() {
        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(a), ImmutableMap.of("key", b));

        assertEquals(Optional.of(a), arguments.positionalOrKeyword("key", Expression.class));
        assertEquals(Optional.of(b), arguments.positionalOrKeyword("key", Expression.class));
        assertEquals(Optional.empty(), arguments.positionalOrKeyword("key", Expression.class));

        verify(a).cast(Expression.class);
        verify(b).cast(Expression.class);
    }

    @Test
    public void throwUnlessEmpty() {
        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(), ImmutableMap.of());

        arguments.throwUnlessEmpty("foo");
    }

    @Test
    public void throwUnlessEmptySingular() {
        expected.expect(IllegalStateException.class);
        expected.expectMessage("foo: has trailing argument a and keyword b");

        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(a), ImmutableMap.of("b", b));

        arguments.throwUnlessEmpty("foo");
    }

    @Test
    public void throwUnlessEmptyPlural() {
        expected.expect(IllegalStateException.class);
        expected.expectMessage("foo: has trailing arguments [a, b] and keywords [a, b]");

        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(a, b), ImmutableMap.of("a", a, "b", b));

        arguments.throwUnlessEmpty("foo");
    }

    @Test
    public void throwUnlessEmptyArguments() {
        expected.expect(IllegalStateException.class);
        expected.expectMessage("foo: has trailing arguments [a, b]");

        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(a, b), ImmutableMap.of());

        arguments.throwUnlessEmpty("foo");
    }

    @Test
    public void throwUnlessEmptyKeywords() {
        expected.expect(IllegalStateException.class);
        expected.expectMessage("foo: has trailing keywords [a, b]");

        final AggregationArguments arguments =
            new AggregationArguments(ImmutableList.of(), ImmutableMap.of("a", a, "b", b));

        arguments.throwUnlessEmpty("foo");
    }
}
