package com.spotify.heroic.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import lombok.Getter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.backend.TimeSeries;

public class TimeSeriesResponse {
    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator gen,
                SerializerProvider provider) throws IOException,
                JsonProcessingException {

            gen.writeStartArray();

            final List<TimeSeries> timeseries = (List<TimeSeries>) value;

            for (final TimeSeries t : timeseries) {
                gen.writeStartObject();
                gen.writeFieldName("key");
                gen.writeString(t.getKey());

                gen.writeFieldName("tags");
                gen.writeStartObject();

                for (final Map.Entry<String, String> entry : t.getTags()
                        .entrySet()) {
                    gen.writeFieldName(entry.getKey());
                    gen.writeString(entry.getValue());
                }

                gen.writeEndObject();
                gen.writeEndObject();
            }

            gen.writeEndArray();
        }
    }

    @Getter
    @JsonSerialize(using = ResultSerializer.class)
    private final List<TimeSeries> result;

    public TimeSeriesResponse(final List<TimeSeries> result) {
        this.result = result;
    }
}
