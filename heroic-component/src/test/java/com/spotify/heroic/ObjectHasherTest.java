package com.spotify.heroic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;

public class ObjectHasherTest {
    private ObjectHasher h1;
    private ObjectHasher h2;
    private ObjectHasher h3;

    @Before
    public void setup() {
        h1 = newObjectHasher();
        h2 = newObjectHasher();
        h3 = newObjectHasher();
    }

    private ObjectHasher newObjectHasher() {
        final Hasher h = Hashing.murmur3_128().newHasher();
        return new ObjectHasher(h);
    }

    /**
     * This tries to verify that two chained lists do not have overlapping hashes.
     */
    @Test
    public void testChainedLists() {
        final Consumer<List<Boolean>> f1 = h1.list(h1.bool());
        final Consumer<List<Boolean>> f2 = h2.list(h2.bool());

        f1.accept(ImmutableList.of(true, true));
        f1.accept(ImmutableList.of(true, true));

        f2.accept(ImmutableList.of(true));
        f2.accept(ImmutableList.of(true, true, true));

        assertNotEquals(h1.result(), h2.result());
    }

    private static final class Foo {
        private final int foo;

        Foo(final int foo) {
            this.foo = foo;
        }

        void hashTo(final ObjectHasher hasher) {
            hasher.putObject(getClass(), () -> {
                hasher.putField("foo", foo, hasher.integer());
            });
        }
    }

    private static final class Bar {
        private final int foo;

        Bar(final int foo) {
            this.foo = foo;
        }

        void hashTo(final ObjectHasher hasher) {
            hasher.putObject(getClass(), () -> {
                hasher.putField("foo", foo, hasher.integer());
            });
        }
    }

    /**
     * Test that classes with the same name and topology matches.
     * And classes with the same topology, but different name does not.
     */
    @Test
    public void testSameClass() {
        final Consumer<Foo> c1 = h1.with(Foo::hashTo);
        final Consumer<Foo> c2 = h2.with(Foo::hashTo);
        final Consumer<Bar> c3 = h3.with(Bar::hashTo);

        c1.accept(new Foo(42));
        c2.accept(new Foo(42));
        c3.accept(new Bar(42));

        assertEquals(h1.result(), h2.result());
        assertNotEquals(h1.result(), h3.result());
    }

    /**
     * Test that hashing methods are machine-independent for integers.
     */
    @Test
    public void testMachineIndependentInteger() {
        h1.integer().accept(42);
        assertEquals("5faea73cbeb34d9f8e93ca28ecdb6941", h1.result());
    }

    /**
     * Test that hashing methods are machine-independent for longs.
     */
    @Test
    public void testMachineIndependentLongValue() {
        h1.longValue().accept(42L);
        assertEquals("be30e14e8622009c1a006029639373d4", h1.result());
    }

    /**
     * Test that hashing methods are machine-independent for booleans.
     */
    @Test
    public void testMachineIndependentBoolean() {
        h1.bool().accept(true);
        assertEquals("4fcfcd693ceac8e952e46542fad4338d", h1.result());
    }

    /**
     * Test that hashing methods are machine-independent for doubles.
     */
    @Test
    public void testMachineIndependentDoubles() {
        h1.doubleValue().accept(Double.longBitsToDouble(Long.MIN_VALUE));
        assertEquals("5858160d1e93a87ecab5dd1bada7c682", h1.result());
    }

    /**
     * Test that hashing methods are machine-independent for optionals.
     */
    @Test
    public void testMachineIndependentOptional() {
        h1.optional(h1.bool()).accept(Optional.of(true));
        assertEquals("8c286185b933e48caadd9fc05cbe3e34", h1.result());
        h2.optional(h2.bool()).accept(Optional.empty());
        assertEquals("6180982693d0825e32c5b8362f667d8e", h2.result());
    }
}
