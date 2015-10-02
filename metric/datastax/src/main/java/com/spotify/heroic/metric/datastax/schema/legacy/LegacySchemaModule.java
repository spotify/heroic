package com.spotify.heroic.metric.datastax.schema.legacy;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Exposed;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.spotify.heroic.metric.datastax.schema.Schema;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;

import eu.toolchain.async.AsyncFramework;

public class LegacySchemaModule implements SchemaModule {
    public static final String DEFAULT_KEYSPACE = "heroic";

    private final String keyspace;

    @JsonCreator
    public LegacySchemaModule(@JsonProperty("keyspace") String keyspace) {
        this.keyspace = Optional.ofNullable(keyspace).orElse(DEFAULT_KEYSPACE);
    }

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            @Exposed
            public Schema schema(final AsyncFramework async) {
                return new LegacySchema(async, keyspace);
            }

            @Override
            protected void configure() {
            };
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String keyspace;

        public Builder keyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public SchemaModule build() {
            return new LegacySchemaModule(keyspace);
        }
    }
}