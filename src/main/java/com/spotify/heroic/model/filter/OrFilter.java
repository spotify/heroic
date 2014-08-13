package com.spotify.heroic.model.filter;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import org.apache.commons.lang.StringUtils;

@Data
public class OrFilter implements Filter {
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
		final List<Filter> statements = new ArrayList<Filter>(
				this.statements.size());

		for (final Filter f : this.statements) {
			if (f instanceof OrFilter) {
				final OrFilter and = (OrFilter) f.optimize();

				for (final Filter statement : and.getStatements())
					statements.add(statement);

				continue;
			}

			statements.add(f.optimize());
		}

		if (statements.size() == 1)
			return statements.get(0);

		return new OrFilter(statements);
	}
}
