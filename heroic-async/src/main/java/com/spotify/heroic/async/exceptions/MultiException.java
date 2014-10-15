package com.spotify.heroic.async.exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import lombok.Data;
import lombok.Getter;

import org.slf4j.Logger;

public class MultiException extends Exception {
    private static final long serialVersionUID = -8580908536641494443L;

    @Getter
    private final Collection<Exception> causes;

    public MultiException(Collection<Exception> causes) {
        super(formatMessage(causes));
        this.causes = causes;
    }

    private static String formatMessage(Collection<Exception> causes) {
        final StringBuilder builder = new StringBuilder();

        int i = 0;

        for (final Exception e : causes) {
            builder.append(e.getMessage());

            if (++i < causes.size())
                builder.append(", ");
        }

        return builder.toString();
    }

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

    @Data
    public static class NestedException {
        private final int count;
        private final int localCount;
        private final Exception e;
    }

    public static void logError(Logger log, Exception root) {
        int count = 1;

        final Queue<NestedException> queue = new LinkedList<>();
        queue.add(new NestedException(0, 0, root));

        while (queue.size() > 0) {
            final NestedException nested = queue.poll();

            final Exception e = nested.getE();

            if (!(e instanceof MultiException)) {
                if (nested.getCount() != 0) {
                    log.error("Exception#{} in MultiException#{} caught: {}", nested.getLocalCount(),
                            nested.getCount(), e.getMessage(), e);
                } else {
                    log.error("Exception#{} caught: {}", nested.getLocalCount(), e.getMessage(), e);
                }

                continue;
            }

            final MultiException me = (MultiException) e;
            log.error("MultiException#{} caught: {}", count, e.getMessage(), e);

            int localCount = 1;

            for (final Exception candidate : me.getCauses()) {
                queue.add(new NestedException(count, localCount++, candidate));
            }

            ++count;
        }
    }
}
