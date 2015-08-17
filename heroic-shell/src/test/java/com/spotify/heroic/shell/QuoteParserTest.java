package com.spotify.heroic.shell;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class QuoteParserTest {
    private void test(final Case c) throws QuoteParserException {
        Assert.assertEquals(c.expected, QuoteParser.parse(c.input));
    }

    @Test
    public void testBasic() throws QuoteParserException {
        test(Case.of("\"hello world\"", "hello world"));
        test(Case.of("'hello world'", "hello world"));
        test(Case.of("default  split", "default", "split"));
    }

    @Test
    public void testUnicode() throws QuoteParserException {
        test(Case.of("a \\u0020 b", "a", "\u0020", "b"));
        test(Case.of("a \\u2000 b", "a", "\u2000", "b"));
        test(Case.of("a \\u9999 b", "a", "\u9999", "b"));
    }

    private static class Case {
        private final String input;
        private final List<String> expected;

        private Case(String input, List<String> expected) {
            this.input = input;
            this.expected = expected;
        }

        public static Case of(String input, String... expected) {
            return new Case(input, ImmutableList.copyOf(expected));
        }
    }
}
