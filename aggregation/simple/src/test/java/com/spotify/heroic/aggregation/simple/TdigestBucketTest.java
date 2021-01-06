package com.spotify.heroic.aggregation.simple;

import com.google.protobuf.ByteString;
import com.spotify.heroic.metric.DistributionPoint;
import com.spotify.heroic.metric.HeroicDistribution;
import com.tdunning.math.stats.TDigest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


public class TdigestBucketTest {
    private static final Map<String, String> TAGS = new HashMap<>();
    private long timeStamp = System.currentTimeMillis();
    private TDigest tDigest ;
    private double [] data1 = {0.0,1.0,2.0,3.0,4.0,5,0};
    private double [] data2 = {0.7, 0.8, 0.9} ;



    @Test
    public void testZeroValue(){
        final TdigestMergingBucket b = new TdigestMergingBucket(timeStamp);
        TDigest val = b.value();
        Assert.assertEquals(0,val.size());
    }

    @Test
    public void testCount() throws IOException {
        final TdigestMergingBucket b = new TdigestMergingBucket(timeStamp);
        b.updateDistributionPoint(TAGS, createDistributionPoint(data1) );
        Assert.assertEquals(data1.length , b.value().size());
        b.updateDistributionPoint(TAGS, createDistributionPoint(data2));
        Assert.assertEquals(data1.length + data2.length , b.value().size());
    }

    private DistributionPoint createDistributionPoint(double [] data){
        tDigest = TDigest.createDigest(100.0);
        Arrays.stream(data).forEach(d -> tDigest.add(d));
        ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.smallByteSize());
        tDigest.asSmallBytes(byteBuffer);
        ByteString byteString = ByteString.copyFrom(byteBuffer.array());
        return DistributionPoint.create( HeroicDistribution.create(byteString), System.currentTimeMillis());
    }
}
