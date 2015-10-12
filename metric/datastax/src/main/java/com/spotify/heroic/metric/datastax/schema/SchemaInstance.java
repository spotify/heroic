package com.spotify.heroic.metric.datastax.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.metric.datastax.TypeSerializer;
import com.spotify.heroic.metric.datastax.schema.Schema.PreparedFetch;

import eu.toolchain.async.Transform;

public interface SchemaInstance {
    public TypeSerializer<MetricsRowKey> rowKey();

    public List<PreparedFetch> ranges(final Series series, final DateRange range) throws IOException;

    public BoundStatement keysPaging(final Optional<ByteBuffer> first, final int limit);

    public BoundStatement deleteKey(ByteBuffer k);

    public BoundStatement countKey(ByteBuffer k);

    public WriteSession writeSession();

    public Transform<Row, BackendKey> keyConverter();

    public static interface WriteSession {
        public BoundStatement writePoint(Series series, Point d) throws IOException;
    }
}