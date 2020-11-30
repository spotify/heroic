package com.spotify.heroic.aggregation.simple;

import com.google.protobuf.ByteString;
import com.spotify.heroic.metric.DistributionPoint;
import com.spotify.heroic.metric.HeroicDistribution;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class DistributionPointUtils {

    public static DistributionPoint createDistributionPoint(final double [] data, long timestamp){
        TDigest tDigest = TDigest.createDigest(100.0);
        Arrays.stream(data).forEach(tDigest::add);
        ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.smallByteSize());
        tDigest.asSmallBytes(byteBuffer);
        ByteString byteString = ByteString.copyFrom(byteBuffer.array());
        return DistributionPoint.create( HeroicDistribution.create(byteString), timestamp);
    }
}
