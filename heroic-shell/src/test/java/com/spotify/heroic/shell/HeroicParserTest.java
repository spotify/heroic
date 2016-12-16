package com.spotify.heroic.shell;

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class HeroicParserTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final HeroicParser parser = new HeroicParser();

    @Test
    public void testBasic() throws QuoteParserException {
        assertEquals(asList("a", "hello", "world", "b"), parse("a hello world b"));
    }

    @Test
    public void testDoubleQuotes() {
        assertEquals(asList("a", "hello world", "b"), parse("a \"hello world\" b"));
    }

    @Test
    public void testSingleQuotes() {
        assertEquals(asList("a", "hello world", "b"), parse("a \'hello world\' b"));
    }

    @Test
    public void testMultipleWhitespace() {
        assertEquals(asList("a", "hello", "world", "b"), parse("a hello   world b"));
    }

    @Test
    public void testComment() {
        assertEquals(asList("a", "hello world", "b"),
            parse("a \"hello\"#ignored \\\n\" world\" b"));
    }

    @Test
    public void testSemiColonSplit() {
        assertEquals(asList("a", "hello", null, "world", "b"), parse("a hello;world b"));
    }

    @Test
    public void testQuoteSemiColon() {
        assertEquals(asList("a", "hello ; world", "b"), parse("a \"hello ; world\" b"));
    }

    @Test
    public void testNewlineIgnored() {
        assertEquals(asList("a", "hello world", "b"), parse("a \"hello \nworld\" b"));
    }

    @Test
    public void testContinuation() {
        expected.expect(EOFError.class);
        parse("hello\\");
    }

    @Test
    public void testOpenQuote() {
        expected.expect(EOFError.class);
        parse("\"hello");
    }

    @Test
    public void testBadEscape() {
        expected.expect(SyntaxError.class);
        parse("\"\\ulool\"");
    }

    @Test
    public void testUnicode() throws QuoteParserException {
        assertEquals(asList("a", "\u0000", "b"), parse("a \"\\u0000\" b"));
        assertEquals(asList("a", "\u0020", "b"), parse("a \"\\u0020\" b"));
        assertEquals(asList("a", "\u2000", "b"), parse("a \"\\u2000\" b"));
        assertEquals(asList("a", "\u9999", "b"), parse("a \"\\u9999\" b"));
        assertEquals(asList("a", "\uffff", "b"), parse("a \"\\uffff\" b"));
        assertEquals(asList("a", "\uFFFF", "b"), parse("a \"\\uFFFF\" b"));
    }

    @Test
    public void testWordCursor() {
        // cursor is relative to accepted buffers
        final ParsedLine line = parser.parse("h\"w\"", 4);
        assertEquals(2, line.wordCursor());
    }

    private List<String> parse(final String input) {
        return parser.parse(input, 0).words();
    }
}
