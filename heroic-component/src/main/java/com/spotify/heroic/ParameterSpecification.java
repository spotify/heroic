package com.spotify.heroic;

import java.io.PrintWriter;
import java.text.BreakIterator;
import java.util.Locale;
import java.util.Optional;

import lombok.Data;

@Data
public class ParameterSpecification {
    private final String name;
    private final String description;
    private final Optional<String> metavar;

    public static ParameterSpecification parameter(String name, String description) {
        return new ParameterSpecification(name, description, Optional.empty());
    }

    public static ParameterSpecification parameter(String name, String description, String metavar) {
        return new ParameterSpecification(name, description, Optional.of(metavar));
    }

    public void printHelp(final PrintWriter out, final String prefix, final int width) {
        if (metavar.isPresent()) {
            out.println(prefix + name + "=" + metavar.get());
        } else {
            out.println(prefix + name);
        }

        printWrapped(out, prefix + "    ", width, description);
    }

    public static void printWrapped(final PrintWriter out, final String prefix, final int width, final String doc) {
        final BreakIterator boundary = BreakIterator.getLineInstance(Locale.US);
        boundary.setText(doc);

        int start = boundary.first();
        int end = boundary.next();
        int line = prefix.length();

        final int maxWidth = width - prefix.length();

        out.print(prefix);

        while (end != BreakIterator.DONE) {
           final String word = doc.substring(start,end);

           line = line + word.length();

           if (line >= maxWidth) {
              out.println();
              out.print(prefix);
              line = word.length() + prefix.length();
           }

           out.print(word);
           start = end;
           end = boundary.next();
        }

        out.println();
    }
}