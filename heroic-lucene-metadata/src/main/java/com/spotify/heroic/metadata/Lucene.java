package com.spotify.heroic.metadata;

import java.io.IOException;

import javax.inject.Named;
import javax.inject.Singleton;

import lombok.RequiredArgsConstructor;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.lucene.LuceneMetadataBackend;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.MetadataManagerReporter;

@RequiredArgsConstructor
public final class Lucene implements MetadataModule {
    private final String id;

    private final String indexDirectory;

    private static final String DEFAULT_INDEX_DIRECTORY = "./lucene";

    @JsonCreator
    public static Lucene create(@JsonProperty("id") String id,
            @JsonProperty("indexDirectory") String indexDirectory) {
        if (indexDirectory == null) {
            indexDirectory = DEFAULT_INDEX_DIRECTORY;
        }

        return new Lucene(id, indexDirectory);
    }

    @Override
    public Module module(final Key<MetadataBackend> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public MetadataBackendReporter reporter(MetadataManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            @Named("directory")
            public Directory directory() throws IOException {
                return new RAMDirectory();
            }

            @Override
            protected void configure() {
                bind(key).to(LuceneMetadataBackend.class);
                expose(key);
            }
        };
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("lucene#%d", i);
    }
}
