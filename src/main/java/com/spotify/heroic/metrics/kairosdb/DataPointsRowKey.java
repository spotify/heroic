package com.spotify.heroic.metrics.kairosdb;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.netflix.astyanax.serializers.AbstractSerializer;

@ToString(of = { "metricName", "timestamp", "tags" })
@EqualsAndHashCode(of = { "metricName", "timestamp", "tags" })
class DataPointsRowKey {
    public static final long MAX_WIDTH = 1814400000L;
    private static final HashMap<String, String> EMPTY_TAGS = new HashMap<String, String>();

    public static class Serializer extends AbstractSerializer<DataPointsRowKey> {
        public static final Serializer instance = new Serializer();

        public static Serializer get() {
            return instance;
        }

        public static final Charset UTF8 = Charset.forName("UTF-8");

        @Override
        public ByteBuffer toByteBuffer(DataPointsRowKey dataPointsRowKey) {
            int size = 8; // size of timestamp
            byte[] metricName = dataPointsRowKey.getMetricName().getBytes(UTF8);
            size += metricName.length;
            size++; // Add one for null at end of string
            byte[] tagString = generateTagString(
                    new TreeMap<String, String>(dataPointsRowKey.getTags()))
                    .getBytes(UTF8);
            size += tagString.length;

            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.put(metricName);
            buffer.put((byte) 0x0);
            buffer.putLong(dataPointsRowKey.getTimestamp());
            buffer.put(tagString);

            buffer.flip();

            return buffer;
        }

        private String generateTagString(SortedMap<String, String> tags) {
            final StringBuilder buffer = new StringBuilder();

            for (Map.Entry<String, String> entry : tags.entrySet()) {
                buffer.append(escape(entry.getKey())).append("=")
                        .append(escape(entry.getValue())).append(":");
            }

            return buffer.toString();
        }

        private String escape(String value) {
          if (value == null)
              return null;

          value = value.replace(":", ".");
          value = value.replace(" ", "_");
          return value.replace("=", "_");
        }

        private Map<String, String> parseTags(byte[] tagsBuffer) {
            final Map<String, String> tags = new HashMap<String, String>();

            final String tagsString = new String(tagsBuffer, UTF8);

            int mark = 0;
            String currentKey = null;
            String currentValue;

            for (int position = 0; position < tagsString.length(); position++) {
                if (currentKey == null) {
                    if (tagsString.charAt(position) != '=') {
                        continue;
                    }

                    currentKey = tagsString.substring(mark, position);
                    mark = position + 1;
                } else {
                    if (tagsString.charAt(position) != ':') {
                        continue;
                    }

                    currentValue = tagsString.substring(mark, position);
                    mark = position + 1;

                    tags.put(currentKey, currentValue);
                    currentKey = null;
                    currentValue = null;
                }
            }

            return tags;
        }

        @Override
        public DataPointsRowKey fromByteBuffer(ByteBuffer byteBuffer) {
            int start = byteBuffer.position();
            byteBuffer.mark();
            // Find null
            while (byteBuffer.get() != 0x0)
                ;

            int nameSize = (byteBuffer.position() - start) - 1;
            byteBuffer.reset();

            byte[] metricNameBuffer = new byte[nameSize];
            byteBuffer.get(metricNameBuffer);
            byteBuffer.get(); // Skip the null

            long timestamp = byteBuffer.getLong();
            byte[] tagsBuffer = new byte[byteBuffer.remaining()];
            byteBuffer.get(tagsBuffer);

            final String metricName = new String(metricNameBuffer, UTF8);
            final Map<String, String> tags = parseTags(tagsBuffer);

            return new DataPointsRowKey(metricName, timestamp, tags);
        }
    }

    @Getter
    private final String metricName;
    @Getter
    private final long timestamp;
    @Getter
    private final Map<String, String> tags;

    public DataPointsRowKey(String metricName, long timestamp) {
        this(metricName, timestamp, EMPTY_TAGS);
    }

    public DataPointsRowKey(String metricName, long timestamp,
            Map<String, String> tags) {
        this.metricName = metricName;
        this.timestamp = timestamp;
        this.tags = tags;
    }

    /**
     * Get the time bucket associated with the specified date.
     * 
     * @param date
     * @return The bucket for the specified date.
     */
    public static long getTimeBucket(long date) {
        return date - (date % MAX_WIDTH);
    }
}