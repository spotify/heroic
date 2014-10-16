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
import com.spotify.heroic.metric.model.ShardedMetricGroup;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Statistics;

@RequiredArgsConstructor
public class QueryMetricsResponse {
    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {

            final List<ShardedMetricGroup> result = (List<ShardedMetricGroup>) value;

            g.writeStartArray();

            for (final ShardedMetricGroup group : result) {
                final Map<String, String> tags = group.getGroup();
                final List<DataPoint> datapoints = group.getDatapoints();

                g.writeStartObject();
                g.writeFieldName("hash");
                g.writeString(Integer.toHexString(group.hashCode()));

                g.writeFieldName("key");
                g.writeNull();

                g.writeFieldName("shard");
                g.writeObject(group.getShard());

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
    private final List<ShardedMetricGroup> result;

    @Getter
    private final Statistics statistics;

    @Getter
    private final List<RequestError> errors;
}
