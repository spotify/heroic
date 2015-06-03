package com.spotify.heroic.filter.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.apache.commons.lang3.StringUtils;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MultiArgumentsFilterBuilder;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "statements" }, doNotUseGetters = true)
public class OrFilterImpl implements Filter.Or {
    public static final String OPERATOR = "or";

    private final List<Filter> statements;

    final MultiArgumentsFilterBuilder<Filter.Or, Filter> BUILDER = new MultiArgumentsFilterBuilder<Filter.Or, Filter>() {
        @Override
        public Filter.Or build(Collection<Filter> terms) {
            return new OrFilterImpl(new ArrayList<>(terms));
        }
    };

    @Override
    public Filter optimize() {
        return optimize(flatten(this.statements));
    }

    private static SortedSet<Filter> flatten(final Collection<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>();

        for (final Filter f : statements) {
            final Filter o = f.optimize();

            if (o == null)
                continue;

            if (o instanceof Filter.Or) {
                result.addAll(((Filter.Or) o).terms());
                continue;
            }

            if (o instanceof Filter.Not) {
                final Filter.Not not = (Filter.Not) o;

                if (not.first() instanceof Filter.And) {
                    result.addAll(collapseNotAnd((Filter.And) not.first()));
                    continue;
                }
            }

            result.add(o);
        }

        return result;
    }

    private static List<Filter> collapseNotAnd(Filter.And first) {
        final List<Filter> result = new ArrayList<>();

        for (final Filter term : first.terms())
            result.add(new NotFilterImpl(term).optimize());

        return result;
    }

    private static Filter optimize(SortedSet<Filter> statements) {
        final SortedSet<Filter> result = new TreeSet<>();

        root:
        for (final Filter f : statements) {
            if (f instanceof Filter.Not) {
                final Filter.Not not = (Filter.Not) f;

                if (statements.contains(not.first()))
                    return TrueFilterImpl.get();

                result.add(f);
                continue;
            }

            if (f instanceof Filter.StartsWith) {
                final Filter.StartsWith outer = (Filter.StartsWith) f;

                for (final Filter inner : statements) {
                    if (inner.equals(outer))
                        continue;

                    if (inner instanceof Filter.StartsWith) {
                        final Filter.StartsWith starts = (Filter.StartsWith) inner;

                        if (!outer.first().equals(starts.first()))
                            continue;

                        if (FilterComparatorUtils.prefixedWith(outer.second(), starts.second()))
                            continue root;
                    }
                }

                result.add(f);
                continue;
            }

            // all ok!
            result.add(f);
        }

        if (result.isEmpty())
            return TrueFilterImpl.get();

        if (result.size() == 1)
            return result.iterator().next();

        return new OrFilterImpl(new ArrayList<>(result));
    }

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
    public String operator() {
        return OPERATOR;
    }

    @Override
    public List<Filter> terms() {
        return statements;
    }

    public static Filter of(Filter... filters) {
        return new OrFilterImpl(Arrays.asList(filters));
    }

    @Override
    public int compareTo(Filter o) {
        if (!Filter.Or.class.isAssignableFrom(o.getClass()))
            return operator().compareTo(o.operator());

        final Filter.Or other = (Filter.Or) o;
        return FilterComparatorUtils.compareLists(terms(), other.terms());
    }
}