package com.spotify.heroic;

import static com.google.common.math.Quantiles.percentiles;


import com.google.protobuf.ByteString;
import com.spotify.heroic.metric.HeroicDistribution;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.distribution.ParetoDistribution;


public class HeroicDistributionGenerator {

  static HeroicDistribution generateOne(){
       List<Double> data = Data.getRandomData(1000);
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

     static RandomData generateRandomDataset(int count){
      List<Double> data = Data.getRandomData(count);
      Map<Integer, Double> res = computePercentile(data);
      Collections.sort(data);
      return new RandomData(data, res);
     }

     static Map<Integer,Double>computePercentile(List<Double> data){
       TDigest tdigest = MergingDigest.createDigest(100.0);
       recordValue(tdigest, data);
       Map<Integer, Double> map = new HashMap<>();
       map.put(99, tdigest.quantile(0.99));
       map.put(75, tdigest.quantile(0.75));
       map.put(50, tdigest.quantile(0.50));
       return map;
     }


    public static class Data {
         static List<Double> getRandomData(int count) {
            ParetoDistribution pareto = new ParetoDistribution(5, 1);
            List<Double> list = new ArrayList<>();
            while (count-- > 0) {
                list.add(pareto.sample());
            }
            return list;
        }
    }


}
