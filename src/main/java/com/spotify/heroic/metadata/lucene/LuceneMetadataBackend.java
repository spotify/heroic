package com.spotify.heroic.metadata.lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataOperationException;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.model.Series;

@Slf4j
@ToString
public class LuceneMetadataBackend implements MetadataBackend {
    private final StandardAnalyzer analyzer = new StandardAnalyzer(
            Version.LUCENE_4_9);

    @Inject
    @Named("directory")
    private Directory directory;

    @Override
    public void start() throws Exception {
        final Map<String, String> tags1 = new HashMap<String, String>();
        tags1.put("hello", "world");

        final Map<String, String> tags2 = new HashMap<String, String>();
        tags2.put("hello", "biz");

        write(new Series("foo", tags1));
        write(new Series("bar", tags2));
        write(new Series("baz", tags2));
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public Callback<WriteBatchResult> write(Series series)
            throws MetadataOperationException {
        final IndexWriterConfig config = new IndexWriterConfig(
                Version.LUCENE_4_9, analyzer);

        try (final IndexWriter writer = new IndexWriter(directory, config)) {
            final Document doc = LuceneUtils.convert(series);
            writer.addDocument(doc);

            return new ResolvedCallback<WriteBatchResult>(new WriteBatchResult(
                    true, 1));
        } catch (final IOException e) {
            throw new MetadataOperationException(
                    "failed to open index directory", e);
        }
    }

    @Override
    public Callback<WriteBatchResult> writeBatch(List<Series> series)
            throws MetadataOperationException {
        return null;
    }

    @Override
    public Callback<FindTags> findTags(Filter filter)
            throws MetadataOperationException {

        final org.apache.lucene.search.Filter f = LuceneUtils
                .convertFilter(filter);

        final Map<String, Set<String>> tags = new HashMap<>();

        try (final DirectoryReader reader = DirectoryReader.open(directory)) {
            final IndexSearcher searcher = new IndexSearcher(reader);

            final FilteredQuery query = new FilteredQuery(
                    new MatchAllDocsQuery(), f);

            final TopDocs docs = searcher.search(query, Integer.MAX_VALUE);

            for (final ScoreDoc d : docs.scoreDocs) {
                final Document doc = searcher.doc(d.doc);
                log.info("doc: {}", doc);
                LuceneUtils.update(tags, doc);
            }

            return new ResolvedCallback<>(new FindTags(tags, tags.size()));
        } catch (final IOException e) {
            throw new MetadataOperationException(
                    "failed to open index directory", e);
        }
    }

    @Override
    public void write(String id, Series series)
            throws MetadataOperationException {
    }

    @Override
    public Callback<FindSeries> findSeries(Filter filter)
            throws MetadataOperationException {
        return null;
    }

    @Override
    public Callback<DeleteSeries> deleteSeries(Filter filter)
            throws MetadataOperationException {
        return null;
    }

    @Override
    public Callback<FindKeys> findKeys(Filter filter)
            throws MetadataOperationException {
        return null;
    }

    @Override
    public Callback<Void> refresh() {
        return null;
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
