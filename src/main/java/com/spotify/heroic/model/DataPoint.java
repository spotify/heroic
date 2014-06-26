package com.spotify.heroic.model;

import java.io.IOException;

import lombok.Data;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonDeserialize(using = DataPoint.Deserializer.class)
@JsonSerialize(using = DataPoint.Serializer.class)
@Data
public class DataPoint implements Comparable<DataPoint> {
    public static class Deserializer extends JsonDeserializer<DataPoint> {
        @Override
        public DataPoint deserialize(JsonParser p, DeserializationContext c)
                throws IOException, JsonProcessingException {

            if (p.getCurrentToken() != JsonToken.START_ARRAY)
                throw c.mappingException("Expected start of array");

            if (p.nextToken() != JsonToken.VALUE_NUMBER_INT)
                throw c.mappingException("Expected int (timestamp)");

            final Long timestamp = p.readValueAs(Long.class);

            if (p.nextToken() != JsonToken.VALUE_NUMBER_FLOAT)
                throw c.mappingException("Expected float (value)");

            final Double value = p.readValueAs(Double.class);

            return new DataPoint(timestamp, value);
        }
    }

    public static class Serializer extends JsonSerializer<DataPoint> {
        @Override
        public void serialize(DataPoint d, JsonGenerator g,
                SerializerProvider provider) throws IOException,
                JsonProcessingException {
            g.writeStartArray();
            g.writeNumber(d.getTimestamp());

            final double value = d.getValue();

            if (Double.isNaN(value)) {
                g.writeNull();
            } else {
                g.writeNumber(value);
            }

            g.writeEndArray();
        }
    }

    private final long timestamp;
    private final double value;

    @Override
    public int compareTo(DataPoint o) {
        return Long.compare(timestamp, o.timestamp);
    }
}
