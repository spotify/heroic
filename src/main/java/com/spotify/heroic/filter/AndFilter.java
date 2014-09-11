package com.spotify.heroic.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "statements" }, doNotUseGetters = true)
public class AndFilter implements ManyTermsFilter {
    public static final String OPERATOR = "and";

    public static final ManyTermsFilterBuilder<AndFilter> BUILDER = new ManyTermsFilterBuilder<AndFilter>() {
        @Override
        public AndFilter build(Collection<Filter> filters) {
            return new AndFilter(Lists.newArrayList(filters));
        }
    };

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
        return ManyOptimizer.optimize(statements, AndFilter.class,
                TrueFilter.get(), BUILDER);
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public List<Filter> terms() {
        return statements;
    }
}
