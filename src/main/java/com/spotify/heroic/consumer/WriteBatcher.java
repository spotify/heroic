package com.spotify.heroic.consumer;

import java.util.ArrayList;
import java.util.List;

import com.spotify.heroic.model.WriteMetric;

public class WriteBatcher {
	private static final int BATCH_SIZE = 1000;
	private static final long BATCH_THRESHOLD = 1000 * 15;

	private List<WriteMetric> buffer = new ArrayList<>(BATCH_SIZE);
	private long lastFlush = System.currentTimeMillis();

	public synchronized List<WriteMetric> write(WriteMetric entry) {
		final long now = System.currentTimeMillis();

		if (buffer.size() >= BATCH_SIZE || timedOut(now)) {
			final List<WriteMetric> flush = this.buffer;
			this.buffer = new ArrayList<>(BATCH_SIZE);
			this.lastFlush = now;
			return flush;
		}

		this.buffer.add(entry);
		return null;
	}

	private boolean timedOut(long now) {
		return now - lastFlush > BATCH_THRESHOLD;
	}
}
