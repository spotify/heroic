package com.spotify.heroic.test;


import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.spotify.heroic.metric.DistributionPoint;
import com.spotify.heroic.metric.HeroicDistribution;
import com.spotify.heroic.metric.MetricCollection;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;;
import java.util.List;

public class DistributionPoints {
    private final ImmutableList.Builder<DistributionPoint> points = ImmutableList.builder();

    public DistributionPoints p(final long t, final List<Double> data) {
        points.add(createDistributionPoint(t,data));
        return this;
    }

    public MetricCollection build() {
        return MetricCollection.distributionPoints(points.build());
    }

    public static DistributionPoint createDistributionPoint(long timestamp, final List<Double> data){
        TDigest tDigest = TDigest.createDigest(100.0);
        data.forEach(tDigest::add);
        ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.smallByteSize());
        tDigest.asSmallBytes(byteBuffer);
        ByteString byteString = ByteString.copyFrom(byteBuffer.array());
        return DistributionPoint.create( HeroicDistribution.create(byteString), timestamp);
    }
}
