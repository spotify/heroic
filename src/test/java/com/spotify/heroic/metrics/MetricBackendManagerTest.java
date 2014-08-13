package com.spotify.heroic.metrics;

import java.util.List;

import org.junit.Before;
import org.mockito.Mock;

import com.spotify.heroic.statistics.MetricBackendManagerReporter;

public class MetricBackendManagerTest {
	@Mock
	private MetricBackendManagerReporter reporter;
	@Mock
	private List<MetricBackend> metricBackends;

	private static final boolean UPDATE_METADATA = false;
	private static final int GROUP_LIMIT = 42;
	private static final int GROUP_LOAD_LIMIT = 42;

	private MetricBackendManager manager;

	@Before
	public void before() {
		this.manager = new MetricBackendManager(reporter, metricBackends,
				UPDATE_METADATA, GROUP_LIMIT, GROUP_LOAD_LIMIT);
	}
}
