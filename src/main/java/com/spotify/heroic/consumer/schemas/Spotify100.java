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
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledException;
import com.spotify.heroic.async.FailedException;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaException;
import com.spotify.heroic.consumer.WriteBatcher;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteEntry;
import com.spotify.heroic.model.WriteResponse;

@Slf4j
public class Spotify100 implements ConsumerSchema {
	private static final String HOST = "host";
	private static final String KEY = "key";
	private static final String TIME = "time";

	private static final ObjectMapper mapper = new ObjectMapper();

	public static final String SCHEMA_VERSION = "1.0.0";

	private final WriteBatcher batcher = new WriteBatcher();

	@Data
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Metric {
		private final String version;
		private final String key;
		private final String host;
		private final Long time;
		private final Map<String, String> attributes;
		private final Double value;

		@JsonCreator
		public static Metric create(@JsonProperty("version") String version,
				@JsonProperty("key") String key,
				@JsonProperty("host") String host,
				@JsonProperty("time") Long time,
				@JsonProperty("attributes") Map<String, String> attributes,
				@JsonProperty("value") Double value) {
			return new Metric(version, key, host, time, attributes, value);
		}
	}

	@Override
	public void consume(Consumer consumer, byte[] message)
			throws ConsumerSchemaException {
		final Metric metric;

		try {
			metric = mapper.readValue(message, Metric.class);
		} catch (final Exception e) {
			throw new ConsumerSchemaException("Received invalid metric", e);
		}
		if (metric.getValue() == null) {
			throw new ConsumerSchemaException(
					"Metric must have a value but this metric has a null value: "
							+ metric);
		}

		if (metric.getVersion() == null
				|| !SCHEMA_VERSION.equals(metric.getVersion()))
			throw new ConsumerSchemaException(String.format(
					"Invalid version {}, expected {}", metric.getVersion(),
					SCHEMA_VERSION));

		if (metric.getTime() == null)
			throw new ConsumerSchemaException("'" + TIME
					+ "' field must be defined: " + message);

		if (metric.getKey() == null)
			throw new ConsumerSchemaException("'" + KEY
					+ "' field must be defined: " + message);

		final Map<String, String> tags = new HashMap<String, String>(
				metric.getAttributes());
		tags.put(HOST, metric.getHost());

		final TimeSerie timeSerie = new TimeSerie(metric.getKey(), tags);
		final DataPoint datapoint = new DataPoint(metric.getTime(),
				metric.getValue());
		final List<DataPoint> data = new ArrayList<DataPoint>();
		data.add(datapoint);

		final List<WriteEntry> flush = batcher.write(new WriteEntry(timeSerie,
				data));

		if (flush == null) {
			return;
		}

		WriteResponse result;

		try {
			result = consumer.getMetricBackendManager().write(flush).get();
		} catch (final InterruptedException e) {
			throw new ConsumerSchemaException("Write batch interrupted", e);
		} catch (final CancelledException e) {
			throw new ConsumerSchemaException("Write batch cancelled: "
					+ e.getReason(), e);
		} catch (final FailedException e) {
			throw new ConsumerSchemaException("Write batch failed: "
					+ e.getCause());
		}

		log.info(String
				.format("Write batch finished (successful=%d, cancelled=%d, failed=%d)",
						result.getSuccessful(), result.getCancelled().size(),
						result.getFailed().size()));

		for (final CancelReason reason : result.getCancelled()) {
			log.error("Write cancelled: " + reason);
		}

		for (final Exception e : result.getFailed()) {
			log.error("Write failed", e);
		}

		final int failCount = result.getCancelled().size()
				+ result.getFailed().size();

		if (failCount != 0) {
			log.error("Stopping consumer because of failed writes, the service WILL NOT be able to recover from this.");

			try {
				consumer.stop();
			} catch (final Exception e1) {
				log.error("Failed to stop consumer.");
			}
		}
	}
}
