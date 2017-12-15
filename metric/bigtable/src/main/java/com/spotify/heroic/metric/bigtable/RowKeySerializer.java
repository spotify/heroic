package com.spotify.heroic.metric.bigtable;

import com.google.protobuf.ByteString;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import java.io.IOException;
import java.util.SortedMap;

public interface RowKeySerializer {
    void serialize(SerialWriter buffer, RowKey value) throws IOException;

    RowKey deserialize(SerialReader buffer) throws IOException;

    SortedMap<String, String> deserializeResourceFromSuffix(
        final ByteString rowKeyPrefix, final ByteString fullRowKey
    ) throws IOException;
}
