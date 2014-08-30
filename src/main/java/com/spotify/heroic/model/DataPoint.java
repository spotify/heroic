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

            final Long timestamp;

            {
                if (p.nextToken() != JsonToken.VALUE_NUMBER_INT)
                    throw c.mappingException("Expected int (timestamp)");

                timestamp = p.readValueAs(Long.class);
            }

            final Double value;

            switch (p.nextToken()) {
            case VALUE_NUMBER_FLOAT:
                value = p.readValueAs(Double.class);
                break;
            case VALUE_NUMBER_INT:
                value = p.readValueAs(Long.class).doubleValue();
                break;
            case VALUE_NULL:
                value = Double.NaN;
                break;
            default:
                throw c.mappingException("Expected float (value)");
            }

            if (p.nextToken() != JsonToken.END_ARRAY)
                throw c.mappingException("Expected end of array");

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
