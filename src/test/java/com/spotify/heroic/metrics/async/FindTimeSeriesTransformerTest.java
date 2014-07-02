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
import com.spotify.heroic.model.TimeSerie;

public class FindTimeSeriesTransformerTest {
    private static final String REF_KEY = "foo";
    private static final Map<String, String> GROUP_KEY = new HashMap<String, String>();

    static {
        GROUP_KEY.put("a", "b");
    }

    private static final List<String> GROUP_BY = new ArrayList<String>();

    static {
        GROUP_BY.add(REF_KEY);
    }

    private static final Map<String, String> TAGS = new HashMap<String, String>();

    static {
        TAGS.put(REF_KEY, "foovar");
    }

    private static final Map<String, String> REF_TAGS = new HashMap<String, String>(GROUP_KEY);

    static {
        REF_TAGS.put(REF_KEY, "foovar");
    }

    @Test
    public void testEmpty() throws Exception {
        final Set<TimeSerie> timeSeries = new HashSet<TimeSerie>();
        final FindTimeSeries input = new FindTimeSeries(timeSeries, 0);

        final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(GROUP_KEY, GROUP_BY);
        final FindTimeSeriesGroups output = transformer.transform(input);

        final Map<TimeSerie, Set<TimeSerie>> reference = new HashMap<TimeSerie, Set<TimeSerie>>();
        Assert.assertEquals(reference, output.getGroups());
    }

    @Test
    public void testOne() throws Exception {
        final Set<TimeSerie> timeSeries = new HashSet<TimeSerie>();
        timeSeries.add(new TimeSerie(REF_KEY, TAGS));

        final FindTimeSeries input = new FindTimeSeries(timeSeries, 0);

        final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(GROUP_KEY, GROUP_BY);
        final FindTimeSeriesGroups output = transformer.transform(input);

        final Map<TimeSerie, Set<TimeSerie>> reference = new HashMap<TimeSerie, Set<TimeSerie>>();
        reference.put(new TimeSerie(REF_KEY, REF_TAGS), timeSeries);

        Assert.assertEquals(reference, output.getGroups());
    }
}
