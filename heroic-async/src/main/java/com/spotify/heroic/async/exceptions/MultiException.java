package com.spotify.heroic.async.exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MultiException extends Exception {
    private static final long serialVersionUID = -8580908536641494443L;

    @Getter
    private final Collection<Exception> causes;

    public MultiException merge(List<Exception> causes) {
        final List<Exception> c = new ArrayList<>(this.causes);
        c.addAll(causes);
        return new MultiException(c);
    }

    public boolean isEmpty() {
        return causes.isEmpty();
    }

    /**
     * combine a list of exceptions in to possibly one MultiException.
     *
     * @param errors
     *            List of errors to combine.
     * @return A new MultiException which is as optimal as possible.
     */
    public static Exception combine(Collection<Exception> errors) {
        final List<Exception> causes = new ArrayList<>();

        for (final Exception e : errors) {
            if (e instanceof MultiException) {
                final MultiException m = (MultiException) e;
                causes.addAll(m.getCauses());
                continue;
            }

            causes.add(e);
        }

        return new MultiException(causes);
    }
}
