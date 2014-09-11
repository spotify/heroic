package com.spotify.heroic.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class ManyOptimizer {
    public static <T extends ManyTermsFilter> Filter optimize(
            final Collection<Filter> statements, Class<T> type,
            final NoTermFilter finalizer, ManyTermsFilterBuilder<T> builder) {
        final SortedSet<Filter> step1 = optimizeChildren(type, statements);
        final SortedSet<Filter> result = optimizeOrSet(step1, finalizer);

        if (result.isEmpty())
            return finalizer;

        if (result.size() == 1)
            return result.iterator().next();

        return builder.build(new ArrayList<>(result));
    }

    private static SortedSet<Filter> optimizeChildren(
            Class<? extends ManyTermsFilter> type,
            final Collection<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>(FilterComparator.get());

        for (final Filter f : statements) {
            final Filter o = f.optimize();

            if (o == null)
                continue;

            if (type.isAssignableFrom(f.getClass())) {
                final ManyTermsFilter child = type.cast(f);

                for (final Filter statement : child.terms())
                    result.add(statement);

                continue;
            }

            result.add(o);
        }

        return result;
    }

    private static SortedSet<Filter> optimizeOrSet(
            final SortedSet<Filter> statements, NoTermFilter finalizer) {
        final SortedSet<Filter> result = new TreeSet<>(FilterComparator.get());
        final Set<Filter> rejected = new HashSet<Filter>();

        root:
            for (final Filter f : statements) {
                if (rejected.contains(f))
                    continue;

                for (final Filter o : statements) {
                    if (rejected.contains(o))
                        continue root;

                    if (o instanceof NotFilter
                            && f.equals(((NotFilter) o).getFilter())) {
                        result.clear();
                        result.add(finalizer.invert());
                        return result;
                    }
                }

                result.add(f);
            }

        return result;
    }
}
