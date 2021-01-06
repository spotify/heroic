package com.spotify.heroic.aggregation.simple;


import static org.junit.Assert.assertEquals;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.ChainInstance;
import com.spotify.heroic.aggregation.EmptyInstance;
import com.spotify.heroic.aggregation.GroupInstance;
import com.spotify.heroic.aggregation.GroupingAggregation;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.DistributionPoint;
import com.spotify.heroic.metric.HeroicDistribution;
import com.spotify.heroic.metric.TdigestPoint;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class TdigestAggregationTest {
   private static final  double [] data = {0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1};
   private static final long timestamp = 1;

    @Test
    public void testAggregation() {
        final GroupingAggregation g =
            new GroupInstance(Optional.of(ImmutableList.of("site")), EmptyInstance.INSTANCE);

        final AggregationInstance b1 = ChainInstance.of(g,new TdigestInstance(1, 1));

        final Set<Series> series = new HashSet<>();
        final Series s1 = Series.of("foo", ImmutableMap.of("site", "s1", "host", "h1"));
        final Series s2 = Series.of("foo", ImmutableMap.of("site", "s2" , "host", "h2"));
        final Series s3 = Series.of("foo", ImmutableMap.of("site", "s3",  "host", "h3" ));

        series.add(s1);
        series.add(s2);
        series.add(s3);

        final AggregationSession session = b1.session(new DateRange(0, 10));

        session.updateDistributionPoints(s1.getTags(), series,
            ImmutableList.of(createDistributionPoint(data,timestamp),createDistributionPoint(data,timestamp+ 1) ));
        session.updateDistributionPoints(s2.getTags(), series,
            ImmutableList.of(createDistributionPoint(data,timestamp),createDistributionPoint(data,timestamp + 1)));
        session.updateDistributionPoints(s3.getTags(), series,
            ImmutableList.of(createDistributionPoint(data,timestamp), createDistributionPoint(data,timestamp + 1)));

        final List<AggregationOutput> result = session.result().getResult();


        assertEquals(1, result.size());
        assertEquals(2,result.get(0).getMetrics().size());

        //validate recorded data points count
        for (TdigestPoint point : result.get(0).getMetrics().getDataAs(TdigestPoint.class) ){
            assertEquals(3*data.length, point.value().size());
        }

    }

    static DistributionPoint createDistributionPoint(final double [] data, long timestamp){
        TDigest tDigest = TDigest.createDigest(100.0);
        Arrays.stream(data).forEach(tDigest::add);
        ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.smallByteSize());
        tDigest.asSmallBytes(byteBuffer);
        ByteString byteString = ByteString.copyFrom(byteBuffer.array());
        return DistributionPoint.create( HeroicDistribution.create(byteString), timestamp);
    }
}
