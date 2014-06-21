package com.spotify.heroic.http.model;

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
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public class MetricsResponse {
    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator g,
                SerializerProvider provider) throws IOException,
                JsonProcessingException {

            final Map<TimeSerie, List<DataPoint>> result = (Map<TimeSerie, List<DataPoint>>) value;

            g.writeStartArray();

            for (Map.Entry<TimeSerie, List<DataPoint>> entry : result
                    .entrySet()) {
                final TimeSerie timeSerie = entry.getKey();
                final List<DataPoint> datapoints = entry.getValue();

                g.writeStartObject();
                g.writeFieldName("hash");
                g.writeString(Integer.toHexString(timeSerie.hashCode()));

                g.writeFieldName("key");
                g.writeString(timeSerie.getKey());

                g.writeFieldName("tags");
                g.writeObject(timeSerie.getTags());

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
    private final Map<TimeSerie, List<DataPoint>> result;

    @Getter
    private final Statistics statistics;
}
