package com.spotify.heroic.metric.datastax.schema;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.datastax.schema.legacy.LegacySchema;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchema;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = LegacySchema.class, name = "legacy"),
        @JsonSubTypes.Type(value = NextGenSchema.class, name = "ng") })
public interface Schema {
    public AsyncFuture<Void> configure(final Session session);

    public AsyncFuture<SchemaInstance> instance(final Session session);

    public static interface PreparedFetch {
        public BoundStatement fetch(int limit);

        public Transform<Row, Point> converter();
    }
}