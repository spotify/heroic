package com.spotify.heroic.metadata.lucene;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.PrefixFilter;
import org.elasticsearch.common.lucene.search.MatchAllDocsFilter;
import org.elasticsearch.common.lucene.search.MatchNoDocsFilter;
import org.elasticsearch.common.lucene.search.RegexpFilter;

import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.model.Series;

@Slf4j
public final class LuceneUtils {
    public static final String KEY = "$$";

    public static org.apache.lucene.search.Filter convertFilter(
            final Filter filter) {
        if (filter instanceof TrueFilter) {
            return new MatchAllDocsFilter();
        }

        if (filter instanceof FalseFilter) {
            return new MatchNoDocsFilter();
        }

        if (filter instanceof AndFilter) {
            final AndFilter and = (AndFilter) filter;
            final List<org.apache.lucene.search.Filter> filters = new ArrayList<>(
                    and.getStatements().size());

            for (final Filter stmt : and.getStatements())
                filters.add(convertFilter(stmt));

            return new org.elasticsearch.common.lucene.search.AndFilter(filters);
        }

        if (filter instanceof OrFilter) {
            final OrFilter or = (OrFilter) filter;
            final List<org.apache.lucene.search.Filter> filters = new ArrayList<>(
                    or.getStatements().size());

            for (final Filter stmt : or.getStatements())
                filters.add(convertFilter(stmt));

            return new org.elasticsearch.common.lucene.search.OrFilter(filters);
        }

        if (filter instanceof NotFilter) {
            final NotFilter not = (NotFilter) filter;
            return new org.elasticsearch.common.lucene.search.NotFilter(
                    convertFilter(not.getFilter()));
        }

        if (filter instanceof MatchTagFilter) {
            final MatchTagFilter matchTag = (MatchTagFilter) filter;
            return new TermFilter(new Term(matchTag.getTag(),
                    matchTag.getValue()));
        }

        if (filter instanceof StartsWithFilter) {
            final StartsWithFilter startsWith = (StartsWithFilter) filter;

            return new PrefixFilter(new Term(startsWith.getTag(),
                    startsWith.getValue()));
        }

        if (filter instanceof RegexFilter) {
            final RegexFilter regex = (RegexFilter) filter;
            return new RegexpFilter(new Term(regex.getTag(), regex.getValue()));
        }

        /*
         * if (filter instanceof HasTagFilter) { final HasTagFilter hasTag =
         * (HasTagFilter) filter; return new TermFilter(new
         * Term(matchTag.getTag(), matchTag.getValue())); }
         */

        if (filter instanceof MatchKeyFilter) {
            final MatchKeyFilter matchKey = (MatchKeyFilter) filter;
            return new TermFilter(new Term(KEY, matchKey.getValue()));
        }

        throw new IllegalArgumentException("Invalid filter statement: "
                + filter);
    }

    public static Document convert(Series series) {
        final Document doc = new Document();

        doc.add(new TextField(KEY, series.getKey(), Field.Store.YES));

        for (final Map.Entry<String, String> e : series.getTags().entrySet()) {
            if (KEY.equals(e.getKey())) {
                log.warn("Ignoring tag with reserved name '{}': {}", KEY, e);
                continue;
            }

            doc.add(new TextField(e.getKey(), e.getValue(), Field.Store.YES));
        }

        return doc;
    }

    public static void update(Map<String, Set<String>> tags, Document doc) {
        for (final IndexableField field : doc.getFields()) {
            final String name = field.name();
            final String value = field.stringValue();

            if (KEY.equals(name)) {
                continue;
            }

            Set<String> values = tags.get(name);

            if (values == null) {
                values = new HashSet<>();
                tags.put(name, values);
            }

            values.add(value);
        }
    }
}
