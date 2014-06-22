package com.spotify.heroic.consumer.schemas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaException;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;

@Slf4j
public class Spotify100 implements ConsumerSchema {
    private static final String HOST = "host";
    private static final String KEY = "key";
    private static final String TIME = "time";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String SCHEMA_VERSION = "1.0.0";

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Event {
        private final String version;
        private final String key;
        private final String host;
        private final Long time;
        private final Map<String, String> attributes;
        private final Double value;

        @JsonCreator
        public static Event create(@JsonProperty("version") String version,
                @JsonProperty("key") String key,
                @JsonProperty("host") String host,
                @JsonProperty("time") Long time,
                @JsonProperty("attributes") Map<String, String> attributes,
                @JsonProperty("value") Double value) {
            return new Event(version, key, host, time, attributes, value);
        }
    }

    @Override
    public void consume(Consumer consumer, byte[] message)
            throws ConsumerSchemaException {
        final Event event;

        try {
            event = mapper.readValue(message, Event.class);
        } catch (final Exception e) {
            throw new ConsumerSchemaException("Received invalid event", e);
        }

        if (event.getVersion() == null
                || !SCHEMA_VERSION.equals(event.getVersion()))
            throw new ConsumerSchemaException(String.format(
                    "Invalid version {}, expected {}", event.getVersion(),
                    SCHEMA_VERSION));

        if (event.getTime() == null)
            throw new ConsumerSchemaException("'" + TIME
                    + "' field must be defined: "
                    + message);

        if (event.getKey() == null)
            throw new ConsumerSchemaException("'" + KEY
                    + "' field must be defined: "
                    + message);

        final Map<String, String> tags = new HashMap<String, String>(
                event.getAttributes());
        tags.put(HOST, event.getHost());

        final TimeSerie timeSerie = new TimeSerie(event.getKey(), tags);
        final DataPoint datapoint = new DataPoint(event.getTime(),
                event.getValue());
        final List<DataPoint> datapoints = new ArrayList<DataPoint>();
        datapoints.add(datapoint);

        consumer.getMetricBackendManager().write(timeSerie, datapoints)
        .register(new Callback.Handle<WriteResponse>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                log.error("Write cancelled: " + reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                log.error("Write failed", e);
            }

            @Override
            public void resolved(WriteResponse result) throws Exception {
            }
        });
    }
}
