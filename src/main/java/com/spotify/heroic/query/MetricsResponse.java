package com.spotify.heroic.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.backend.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public class MetricsResponse {
    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator jgen,
                SerializerProvider provider) throws IOException,
                JsonProcessingException {

            final Map<TimeSerie, List<DataPoint>> result = (Map<TimeSerie, List<DataPoint>>) value;

            jgen.writeStartArray();

            for (Map.Entry<TimeSerie, List<DataPoint>> entry : result
                    .entrySet()) {
                final TimeSerie timeSerie = entry.getKey();
                final List<DataPoint> datapoints = entry.getValue();

                jgen.writeStartObject();
                jgen.writeFieldName("hash");
                jgen.writeString(Integer.toHexString(timeSerie.hashCode()));

                jgen.writeFieldName("key");
                jgen.writeString(timeSerie.getKey());

                jgen.writeFieldName("tags");
                jgen.writeObject(timeSerie.getTags());

                jgen.writeFieldName("values");
                jgen.writeStartArray();

                for (final DataPoint d : datapoints) {
                    jgen.writeStartArray();
                    jgen.writeNumber(d.getTimestamp());
                    jgen.writeNumber(d.getValue());
                    jgen.writeEndArray();
                }

                jgen.writeEndArray();
                jgen.writeEndObject();
            }

            jgen.writeEndArray();
        }
    }

    @Getter
    @JsonSerialize(using = ResultSerializer.class)
    private final Map<TimeSerie, List<DataPoint>> result;

    @Getter
    private final Statistics statistics;
}
