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
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public class QueryMetricsResponse {
    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator g,
                SerializerProvider provider) throws IOException,
                JsonProcessingException {

            final Map<Series, List<DataPoint>> result = (Map<Series, List<DataPoint>>) value;

            g.writeStartArray();

            for (Map.Entry<Series, List<DataPoint>> entry : result
                    .entrySet()) {
                final Series series = entry.getKey();
                final List<DataPoint> datapoints = entry.getValue();

                g.writeStartObject();
                g.writeFieldName("hash");
                g.writeString(Integer.toHexString(series.hashCode()));

                g.writeFieldName("key");
                g.writeString(series.getKey());

                g.writeFieldName("tags");
                g.writeObject(series.getTags());

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
    private final Map<Series, List<DataPoint>> result;

    @Getter
    private final Statistics statistics;
}
