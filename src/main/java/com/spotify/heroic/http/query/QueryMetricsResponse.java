package com.spotify.heroic.http.query;

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
import com.spotify.heroic.metric.model.RequestError;
import com.spotify.heroic.metric.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class QueryMetricsResponse {
    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {

            final Map<Map<String, String>, List<DataPoint>> result = (Map<Map<String, String>, List<DataPoint>>) value;

            g.writeStartArray();

            for (final Map.Entry<Map<String, String>, List<DataPoint>> entry : result.entrySet()) {
                final Map<String, String> tags = entry.getKey();
                final List<DataPoint> datapoints = entry.getValue();

                g.writeStartObject();
                g.writeFieldName("hash");
                g.writeString(Integer.toHexString(tags.hashCode()));

                g.writeFieldName("key");
                g.writeNull();

                g.writeFieldName("tags");
                g.writeObject(tags);

                g.writeFieldName("values");
                g.writeObject(datapoints);

                g.writeEndObject();
            }

            g.writeEndArray();
        }
    }

    @Getter
    private final DateRange range;

    @Getter
    @JsonSerialize(using = ResultSerializer.class)
    private final Map<Map<String, String>, List<DataPoint>> result;

    @Getter
    private final Statistics statistics;

    @Getter
    private final List<RequestError> errors;
}
