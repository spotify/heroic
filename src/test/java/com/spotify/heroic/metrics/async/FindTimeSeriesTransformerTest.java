package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.metadata.model.FindTimeSeries;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.model.Series;

public class FindTimeSeriesTransformerTest {
	private static final String REF_KEY = "foo";

	private static final List<String> GROUP_BY = new ArrayList<String>();

	static {
		GROUP_BY.add(REF_KEY);
	}

	private static final Map<String, String> TAGS = new HashMap<String, String>();

	static {
		TAGS.put(REF_KEY, "foovar");
		TAGS.put("other", "barvar");
	}

	private static final Map<String, String> REF_TAGS = new HashMap<String, String>();

	static {
		REF_TAGS.put(REF_KEY, "foovar");
	}

	@Test
	public void testEmpty() throws Exception {
		final Set<Series> series = new HashSet<Series>();
		final FindTimeSeries input = new FindTimeSeries(series, 0);

		final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(
				GROUP_BY);
		final FindTimeSeriesGroups output = transformer.transform(input);

		final Map<Series, Set<Series>> reference = new HashMap<Series, Set<Series>>();
		Assert.assertEquals(reference, output.getGroups());
	}

	@Test
	public void testOne() throws Exception {
		final Set<Series> series = new HashSet<Series>();
		series.add(new Series(REF_KEY, TAGS));

		final FindTimeSeries input = new FindTimeSeries(series, 0);

		final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(
				GROUP_BY);
		final FindTimeSeriesGroups output = transformer.transform(input);

		final Map<Series, Set<Series>> reference = new HashMap<Series, Set<Series>>();
		reference.put(new Series(REF_KEY, REF_TAGS), series);

		Assert.assertEquals(reference, output.getGroups());
	}
}
