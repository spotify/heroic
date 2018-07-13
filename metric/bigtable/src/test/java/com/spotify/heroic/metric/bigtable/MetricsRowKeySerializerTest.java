package com.spotify.heroic.metric.bigtable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import com.spotify.heroic.common.Series;
import eu.toolchain.serializer.HexUtils;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import junit.framework.TestCase;
import org.junit.Assert;

public class MetricsRowKeySerializerTest extends TestCase {
    private Series series = Series.of("key", ImmutableMap.of("from", "123", "to", "4567"));
    private Series seriesResource = Series.of("key", ImmutableMap.of("from", "123", "to", "4567"),
        ImmutableSortedMap.of("resource1", "abc"));

    // V1: Without resource tags
    private static final String EXPECTED_SERIALIZATION_V1 =
        "\u0003\u0003key" + "\u0002\u0004\u0004from\u0003\u0003123" +
            "\u0002\u0002to\u0004\u00044567" + "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001";

    // V2: With resource tags
    private static final String EXPECTED_SERIALIZATION_V2 =
        "\u0003\u0003key" + "\u0002\u0004\u0004from\u0003\u0003123" +
            "\u0002\u0002to\u0004\u00044567" + "\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0001" +
            "\u0001\u0000\u000f\u0001\u0009resource1\u0003abc";

    private final SerializerFramework serializerFramework = TinySerializer.builder().build();
    private final RowKeySerializer serializer = new MetricsRowKeySerializer();

    public void testSerializationIsBackwardsCompatibleV1() throws Exception {
        final RowKey rowKey = new RowKey(series, 1);

        final ByteString serializedBytes = serializer.serializeFull(rowKey);

        Assert.assertArrayEquals(EXPECTED_SERIALIZATION_V1.getBytes(StandardCharsets.UTF_8),
            serializedBytes.toByteArray());
    }

    public void testSerializationIsBackwardsCompatibleV2() throws Exception {
        final RowKey rowKey = new RowKey(seriesResource, 1);

        final ByteString serializedBytes = serializer.serializeFull(rowKey);

        Assert.assertArrayEquals(EXPECTED_SERIALIZATION_V2.getBytes(StandardCharsets.UTF_8),
            serializedBytes.toByteArray());
    }

    public void testDeserializationIsBackwardsCompatibleV1() throws Exception {
        final ByteBuffer serializedBytes =
            ByteBuffer.wrap(EXPECTED_SERIALIZATION_V1.getBytes(StandardCharsets.UTF_8));

        final RowKey rowKey = serializer.deserializeFull(serializedBytes);

        assertEquals(rowKey, new RowKey(series, 1));
    }

    public void testDeserializationIsBackwardsCompatibleV2() throws Exception {
        final ByteBuffer serializedBytes =
            ByteBuffer.wrap(EXPECTED_SERIALIZATION_V2.getBytes(StandardCharsets.UTF_8));

        final RowKey rowKey = serializer.deserializeFull(serializedBytes);

        assertEquals(rowKey, new RowKey(seriesResource, 1));
    }
}
