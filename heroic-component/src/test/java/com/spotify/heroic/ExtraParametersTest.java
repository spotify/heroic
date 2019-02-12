package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Duration;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;

public class ExtraParametersTest {
    final ExtraParameters extra = ExtraParameters.ofList(
        ImmutableList.of("foo.bar=true", "foo.baz=42", "hello=1m", "list=42", "list=hello"));

    @Test
    public void testParameters() {
        assertEquals(of(true), extra.getBoolean("foo.bar"));
        assertEquals(empty(), extra.getBoolean("empty"));
        assertEquals(of(Duration.of(1, TimeUnit.MINUTES)), extra.getDuration("hello"));
    }

    @Test
    public void testScoped() {
        final ExtraParameters scope = extra.scope("foo");
        assertEquals(of(true), scope.getBoolean("bar"));
        assertEquals(of(42), scope.getInteger("baz"));
    }

    @Test
    public void testList() {
        assertEquals(ImmutableList.of("42", "hello"), extra.getAsList("list"));
    }
}
