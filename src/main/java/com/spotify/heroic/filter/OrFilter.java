package com.spotify.heroic.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import lombok.Data;

import org.apache.commons.lang.StringUtils;

@Data
public class OrFilter implements ManyTermsFilter, Comparable<Filter> {
    public static final String OPERATOR = "or";

    private final List<Filter> statements;

    @Override
    public String toString() {
        final List<String> parts = new ArrayList<String>(statements.size() + 1);
        parts.add(OPERATOR);

        for (final Filter statement : statements) {
            if (statement == null) {
                parts.add("<null>");
            } else {
                parts.add(statement.toString());
            }
        }

        return "[" + StringUtils.join(parts, ", ") + "]";
    }

    @Override
    public Filter optimize() {
        final SortedSet<Filter> statements = new TreeSet<Filter>();

        for (final Filter f : this.statements) {
            if (f instanceof OrFilter) {
                final OrFilter or = (OrFilter) f.optimize();

                if (or == null)
                    continue;

                for (final Filter statement : or.getStatements())
                    statements.add(statement);

                continue;
            }

            final Filter o = f.optimize();

            if (o == null)
                continue;

            statements.add(o);
        }

        if (statements.isEmpty())
            return null;

        if (statements.size() == 1)
            return statements.iterator().next();

        return new OrFilter(new ArrayList<>(statements));
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public int compareTo(Filter o) {
        if (o == null)
            return -1;

        if (!(o instanceof Filter))
            return -1;

        if (!(o instanceof OrFilter))
            return operator().compareTo(o.operator());

        return Integer.compare(hashCode(), o.hashCode());
    }

    @Override
    public List<Filter> terms() {
        return statements;
    }
}
