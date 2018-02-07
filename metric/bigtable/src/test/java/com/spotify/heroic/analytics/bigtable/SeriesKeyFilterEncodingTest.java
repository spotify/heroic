package com.spotify.heroic.analytics.bigtable;

import com.spotify.heroic.bigtable.com.google.api.client.util.Charsets;
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import com.spotify.heroic.common.Series;
import eu.toolchain.async.Transform;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.LocalDate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class SeriesKeyFilterEncodingTest {
    private final SeriesKeyEncoding foo = new SeriesKeyEncoding("foo");

    @Mock
    Transform<Series, String> toString;

    @Mock
    Transform<String, Series> fromString;

    @Mock
    Series series;

    final LocalDate date = LocalDate.parse("2016-01-31");

    @Test
    public void testKeyEncoding() throws Exception {
        doReturn("series").when(toString).transform(series);

        final ByteString bytes =
            foo.encode(new SeriesKeyEncoding.SeriesKey(date, series), toString);

        assertEquals(ByteString.copyFrom("foo/2016-01-31/series", Charsets.UTF_8), bytes);

        doReturn(series).when(fromString).transform("series");
        final SeriesKeyEncoding.SeriesKey k = foo.decode(bytes, fromString);

        assertEquals(date, k.getDate());
        assertEquals(series, k.getSeries());

        assertEquals(ByteString.copyFrom("foo/2016-02-01", Charsets.UTF_8),
            foo.rangeKey(date.plusDays(1)));
    }
}
