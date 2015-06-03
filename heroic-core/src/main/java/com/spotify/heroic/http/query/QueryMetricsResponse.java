package com.spotify.heroic.http.query;

import java.io.IOException;
import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.metric.model.RequestError;
import com.spotify.heroic.metric.model.ShardLatency;
import com.spotify.heroic.metric.model.ShardedResultGroup;
import com.spotify.heroic.metric.model.TagValues;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Event;
import com.spotify.heroic.model.Statistics;

@RequiredArgsConstructor
public class QueryMetricsResponse {
    private static final String SERIES = "series";
    private static final String EVENTS = "events";

    public static class ResultSerializer extends JsonSerializer<Object> {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(Object value, JsonGenerator g, SerializerProvider provider) throws IOException,
                JsonProcessingException {

            final List<ShardedResultGroup> result = (List<ShardedResultGroup>) value;

            g.writeStartArray();

            for (final ShardedResultGroup group : result) {
                if (group.getType().equals(DataPoint.class)) {
                    final List<TagValues> tags = group.getGroup();
                    final List<DataPoint> datapoints = (List<DataPoint>) group.getValues();
                    writeDataPoints(g, group, tags, datapoints);
                    continue;
                }

                if (group.getType().equals(Event.class)) {
                    final List<TagValues> tags = group.getGroup();
                    final List<Event> events = (List<Event>) group.getValues();
                    writeEvents(g, group, tags, events);
                    continue;
                }
            }

            g.writeEndArray();
        }

        private void writeDataPoints(JsonGenerator g, final ShardedResultGroup group, final List<TagValues> tags,
                final List<DataPoint> datapoints) throws IOException {
            g.writeStartObject();
            g.writeFieldName("type");
            g.writeString(SERIES);

            g.writeFieldName("hash");
            g.writeString(Integer.toHexString(group.hashCode()));

            g.writeFieldName("shard");
            g.writeObject(group.getShard());

            g.writeFieldName("tags");

            g.writeStartObject();

            for (final TagValues pair : tags) {
                final List<String> values = pair.getValues();

                if (values.size() != 1)
                    continue;

                g.writeFieldName(pair.getKey());
                g.writeString(values.iterator().next());
            }

            g.writeEndObject();

            g.writeFieldName("tagCounts");

            g.writeStartObject();

            for (final TagValues pair : tags) {
                final List<String> values = pair.getValues();

                if (values.size() <= 1)
                    continue;

                g.writeFieldName(pair.getKey());
                g.writeNumber(values.size());
            }

            g.writeEndObject();

            g.writeFieldName("values");
            g.writeObject(datapoints);

            g.writeEndObject();
        }

        private void writeEvents(JsonGenerator g, final ShardedResultGroup group, final List<TagValues> tags,
                final List<Event> events) throws IOException {
            g.writeStartObject();

            g.writeFieldName("type");
            g.writeString(EVENTS);

            g.writeFieldName("hash");
            g.writeString(Integer.toHexString(group.hashCode()));

            g.writeFieldName("shard");
            g.writeObject(group.getShard());

            g.writeFieldName("tags");

            g.writeStartObject();

            for (final TagValues pair : tags) {
                g.writeFieldName(pair.getKey());
                g.writeObject(pair.getValues());
            }

            g.writeEndObject();

            g.writeFieldName("values");
            g.writeObject(events);

            g.writeEndObject();
        }
    }

    @Getter
    private final DateRange range;

    @Getter
    @JsonSerialize(using = ResultSerializer.class)
    private final List<ShardedResultGroup> result;

    @Getter
    private final Statistics statistics;

    @Getter
    private final List<RequestError> errors;

    @Getter
    private final List<ShardLatency> latencies;
}
