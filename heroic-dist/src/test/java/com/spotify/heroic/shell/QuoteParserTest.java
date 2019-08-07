package com.spotify.heroic.shell;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class QuoteParserTest {
    private void test(final Case c) throws QuoteParserException {
        Assert.assertEquals(c.expected, QuoteParser.parse(c.input));
    }

    @Test
    public void testBasic() throws QuoteParserException {
        test(Case.of("\"hello world\"", ImmutableList.of("hello world")));
        test(Case.of("'hello world'", ImmutableList.of("hello world")));
        test(Case.of("default  split", ImmutableList.of("default", "split")));
        test(Case.of("default  split; other part", ImmutableList.of("default", "split"),
            ImmutableList.of("other", "part")));
        test(Case.of("default  split# ignore rest", ImmutableList.of("default", "split")));
    }

    @Test
    public void testUnicode() throws QuoteParserException {
        test(Case.of("a \\u0020 b", ImmutableList.of("a", "\u0020", "b")));
        test(Case.of("a \\u2000 b", ImmutableList.of("a", "\u2000", "b")));
        test(Case.of("a \\u9999 b", ImmutableList.of("a", "\u9999", "b")));
    }

    private static class Case {
        private final String input;
        private final List<List<String>> expected;

        private Case(String input, List<List<String>> expected) {
            this.input = input;
            this.expected = expected;
        }

        public static Case of(String input, List<String> expected) {
            return new Case(input, ImmutableList.of(expected));
        }

        public static Case of(String input, List<String> expected, List<String> expected2) {
            return new Case(input, ImmutableList.of(expected, expected2));
        }
    }
}
