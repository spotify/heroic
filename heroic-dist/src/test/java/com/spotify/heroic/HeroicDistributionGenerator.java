package com.spotify.heroic;

import com.google.protobuf.ByteString;
import com.spotify.heroic.metric.HeroicDistribution;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.distribution.ParetoDistribution;


public class HeroicDistributionGenerator {

  static HeroicDistribution generateOne(){
       List<Double> data = Data.generateParetoDistribution(1000);
       TDigest tDigest =TDigest.createMergingDigest(100);
       recordValue(tDigest, data);
       return HeroicDistribution.create(encode(tDigest));
    }

     static ByteString encode(final TDigest tDigest){
        int capacity = tDigest.byteSize();
        ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);
        tDigest.asBytes(byteBuffer);
        return ByteString.copyFrom(byteBuffer.array());
    }

     static List<HeroicDistribution> GenerateMany(int count){
       List<HeroicDistribution> distributions = new  ArrayList<>();
       for(int i = 0; i < count; i++){
           distributions.add(generateOne());
       }
       return List.copyOf(distributions);
    }

     static void recordValue(final TDigest tDigest, List<Double> data){
       data.forEach(dat -> tDigest.add(dat));
    }


    static class Data {
         static List<Double> generateParetoDistribution(int count) {
            ParetoDistribution pareto = new ParetoDistribution(5, 1);
            List<Double> list = new ArrayList<>();
            while (count-- > 0) {
                list.add(pareto.sample());
            }
            return list;
        }
    }


}
