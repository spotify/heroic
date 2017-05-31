/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.http.render;

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Spread;

import javax.imageio.ImageIO;
import java.awt.BasicStroke;
import java.awt.Color;

import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Vector;

public final class HeatmapUtil {
    private static final List<Color> COLORS = new ArrayList<>();
    public int height;
    public static Vector<Vector> vz ;
    protected static double[][] zvalues;
    protected static TreeMap<Double,TreeMap<Long,Double>> zvt;

    static {
        COLORS.add(Color.BLUE);
    }

    public static BufferedImage createChart(
        final List<ShardedResultGroup> groups, final String title, Map<String, String> highlight,
        Double threshold, int height
    ) throws IOException {
        zvt = new TreeMap<Double,TreeMap<Long,Double>>();
        double min,max;
        min =  0d;
        max = 0d;
        System.out.print("get 1: ");
        for (final ShardedResultGroup resultGroup : groups) {

            final MetricCollection group = (MetricCollection) resultGroup.getMetrics();
            final SeriesValues series =(SeriesValues) SeriesValues.fromSeries(resultGroup.getSeries().iterator());
            TreeMap<Long, Double> zv = (TreeMap<Long, Double>) new TreeMap();
            height = group.size();
            if (group.getType() == MetricType.POINT) {

                final List<Point> data = group.getDataAs(Point.class);


                Vector vv = new Vector();
                vz = new Vector();
                int T=0;
                //Long f ;
                int duration=0;
                Double F = null;
                //TreeMap<Long,Double> zv;
                String k = "";
                String K = "";
                Map<String, SortedSet<String>> tags =(Map<String, SortedSet<String>>) series.getTags();
                if (series.getKeys().size() ==1){

                    k= series.getKeys().iterator().next();
                }

                //F = Double.parseDouble(tags.get("f").iterator().next());
                for(Map.Entry<String,SortedSet<String>> pair : tags.entrySet()) {

                    SortedSet<String> values = pair.getValue();
                    if (values.size() != 1) {
                        continue;
                    }
                    K = pair.getKey();

                    if (k.equals("orfees") && K.equals("f")) {

                        String text = values.iterator().next();
                        F = (Double) (Double.parseDouble(text));
                        //System.out.print("frequence : ");
                        //System.out.println(F);

                        for (final Point d : data) {
                            long t = (long) d.getTimestamp();

                            double v = (double) d.getValue();
                            if (v<min){
                                min = v;
                            }
                            if (v>max){
                                max = v;
                            }
                            //System.out.print("timestamp : ");
                            //System.out.println(t);
                            //System.out.print("value : ");
                            //System.out.println(v);
                            //TreeMap<Long,Double> zv;
                            try{
                                TreeMap<Long, Double> zt = (TreeMap<Long, Double>) zvt.get(F);
                                zt.put(t, v);
                                zv = zt;
                            }catch (Exception e){
                                //System.out.print("new");
                                zv = (TreeMap<Long, Double>) new TreeMap();
                                zv.put(t, v);
                            }
                            //System.out.print(zv);

                            zvt.put(F, zv);
                        }
                    }
                }


                //System.out.print(zvt);

            }
            //System.out.print(group.size());
        }
        int width = zvt.size();
        //System.out.print(zvt );
        //System.out.print(zvt.values());
        //System.out.print(zvt.toString());
        //System.out.print(zvt.values().toArray());
        //Collection c = zvt.values();
        //System.out.println(width);
        //System.out.println(height);
        double[][] jj = new double[height][width];
        byte[] bb = new byte[height*width*4];
        int[] ii = new int[height*width];
        //System.out.println(bb);
        //Iterator itr = c.iterator();
        int i=0;
        Double range = max - min;
        System.out.print("get : ");
        for (Map.Entry<Double,TreeMap<Long,Double>> entry : zvt.entrySet()) {
            //System.out.print("i : ");
            //System.out.println(i);
            int j=0;
           // System.out.println("key ->" + entry.getKey() + ", value->" + entry.getValue());
            for (Map.Entry<Long,Double> e : entry.getValue().entrySet()) {
                //System.out.print("j : ");
                //System.out.println(j);
                //System.out.println("k ->" + e.getKey() + ", v->" + e.getValue());

                //jj[j][i]=e.getValue().doubleValue();
                //
                double dd = e.getValue().doubleValue();
                int norm = (int)(((dd -min) / range));
                Color colorOfYourDataPoint = Color(norm*255, norm*255, norm*255);
                ii[i*height+j]=norm;
                //System.out.print("norm : "+ norm);
                //System.out.print("i : "+ i);
                //System.out.print("j : "+ j);
                //int bits = Float.floatToIntBits(myFloat);
                //byte bits = (((byte) norm));

                //System.out.print("bits : ");
                //System.out.println( bits);
                /**
                byte[] bytes = new byte[4];
                bytes[0] = (byte)(bits & 0xff);
                System.out.print("byte : ");
                System.out.println( bytes[0]);
                bb[i*height+j]= (byte)(bits & 0xff);
                System.out.println( bb[i*height+j]);
                j++;
                bytes[1] = (byte)((bits >> 8) & 0xff);
                bb[i*height+j]= (byte)((bits >> 8) & 0xff);
                j++;
                bytes[2] = (byte)((bits >> 16) & 0xff);
                bb[i*height+j]= (byte)((bits >> 16) & 0xff);
                j++;
                bytes[3] = (byte)((bits >> 24) & 0xff);
                bb[i*height+j]= (byte)((bits >> 24) & 0xff);
                j++;
                System.out.println(bytes.toString());
                */
                //byte[] result = new byte[4];
                //System.out.print("bits : ");
                //System.out.println( bits);
                //result[0] = (byte) ((norm & 0xFF000000) >> 24);
                //bb[i*height+j]=result[0];
                //System.out.println(bb[i*height+j] );
                //j++;

                //result[1] = (byte) ((norm & 0x00FF0000) >> 16);
                //bb[i*height+j]=result[1];
                //System.out.println(bb[i*height+j] );
                //j++;
                //result[2] = (byte) ((norm & 0x0000FF00) >> 8);
                //bb[i*height+j]=result[2];
                //System.out.println(bb[i*height+j] );
                //j++;
                //result[3] = (byte) ((norm & 0x000000FF) >> 0);
                //bb[i*height+j]=result[3];
                //System.out.println(bb[i*height+j] );
                j++;

                //BigInteger.valueOf().toByteArray()

                //bb[i*height+j]= (byte)((bits >> 32) & 0xff);
                //j++;
                //bb[i*height+j]= (byte)((bits >> 40) & 0xff);
                //j++;
                //bb[i*height+j]= (byte)((bits >> 48) & 0xff);
                //j++;
                //bb[i*height+j]= (byte)((bits >> 56) & 0xff);
                //j++;

                //bb[j*width+i]= (byte) e.getValue().doubleValue();
                //System.out.println(e.getValue().doubleValue());
               // j++;
            }

            i++;
        }
        //System.out.print(max);
        //System.out.print(min);
        //System.out.print(range);

        //System.out.print("bb : ");
        //System.out.println(bb.toString());

        // final DateAxis rangeAxis = (DateAxis) plot.getRangeAxis();
        // rangeAxis.setStandardTickUnits(DateAxis.createStandardDateTickUnits());
        double[][] datas = new double[][]{{3,2,3,4,5,6},
                                 {2,3,4,5,6,7},
                                 {3,4,5,6,7,6},
                                 {4,5,6,7,6,5}};

        // Step 1: Create our heat map chart using our data.
        //HeatChart map = new HeatChart(jj);

        // Step 2: Customise the chart.
        //map.setTitle("This is my heat chart title");
        //map.setXAxisLabel("X Axis");
        //map.setYAxisLabel("Y Axis");

        // Step 3: Output the chart to a file.
        //map.saveToFile(new File("java-heat-chart.png"));

        //Image img = map.getChartImage();
        //BufferedImage bimage = new BufferedImage(img.getWidth(null), img.getHeight(null), BufferedImage.TYPE_INT_ARGB);

        // Draw the image on to the buffered image
        //Graphics2D bGr = bimage.createGraphics();
        //bGr.drawImage(img, 0, 0, null);
        //bGr.dispose();
        System.out.print("BufferedImage : ");

        //BufferedImage bI = new BufferedImage(width,height,BufferedImage.TYPE_4BYTE_ABGR);
        //System.out.println(bI);
        // Return the buffered image
        //return bimage;
        //byte[] b = new byte[height*width*4];
        //ByteArrayInputStream bais = new ByteArrayInputStream(bb);
        //bais.read(b);
        //System.out.print("bais : ");
        //System.out.println(bais.available());
        /**
        try {
            bI = ImageIO.read(bais);
            System.out.println(bI);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            //throw new RuntimeException(e);
        }
        */
        System.out.print("after : ");
        int[] pixels = ii;
        BufferedImage convertedGrayscale =  new BufferedImage(width, height, BufferedImage.TYPE_INT_BGR);
        convertedGrayscale.getRaster().setDataElements(0, 0, width, height, pixels);
        System.out.print(convertedGrayscale);
        /**
        try {
            ImageIO.write(convertedGrayscale, "png", new File("converted-grayscale-002.png"));
        }
        catch (IOException e) {
            System.err.println("IOException: " + e);
        }
         */

        //write(bI, "jpg", new File("new-darksouls.jpg"));
        return convertedGrayscale;


    }


}
