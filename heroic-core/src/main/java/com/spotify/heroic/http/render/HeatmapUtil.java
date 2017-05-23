package com.spotify.heroic.http.render;

/**
 * Created by lucile on 09/05/17.
 */

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.ArrayList;
import java.lang.Math;
import java.util.stream.Stream;

import jersey.repackaged.com.google.common.collect.HashBasedTable;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.util.ArrayUtil;

import org.jzy3d.analysis.AbstractAnalysis;
import org.jzy3d.analysis.AnalysisLauncher;
import org.jzy3d.chart.factories.AWTChartComponentFactory;
import org.jzy3d.io.FileImage;
import org.jzy3d.plot3d.rendering.image.GLImage;
import org.jzy3d.colors.ColorMapper;
import org.jzy3d.colors.colormaps.ColorMapRainbow;
import org.jzy3d.maths.Range;
import org.jzy3d.plot3d.builder.Builder;
import org.jzy3d.plot3d.builder.Mapper;
import org.jzy3d.plot3d.builder.concrete.OrthonormalGrid;
import org.jzy3d.plot3d.primitives.Shape;
import org.jzy3d.plot3d.rendering.canvas.Quality;

import static javax.imageio.ImageIO.*;

public class HeatmapUtil {
    //extends AbstractAnalysis


    protected double[] xValues = null;
    protected double[] yValues = null;

    protected double[][] zValues = null;



    public  BufferedImage createChart(
        final List<ShardedResultGroup> groups, final String title, Map<String, String> highlight,
        Double threshold, int height
    ) {
        //List<Double> arval  = new ArrayList();
        TwoDimentionalArrayList<Double> listzValues = new TwoDimentionalArrayList();
        List<Double> listxValues = new ArrayList();
        TreeMap<Double, TreeMap<Double, Double>> zzz = new TreeMap<Double, TreeMap<Double, Double>>();
        List<Double> listyValues = new ArrayList();
        Double maxz = null;
        Double minz = null;
        Double lasttime = null;
        TreeMap lastval = new TreeMap<Double, Double>();
        int size = 0;

        for (final ShardedResultGroup resultGroup : groups) {
            final MetricCollection group = resultGroup.getMetrics();
            final SeriesValues series = SeriesValues.fromSeries(resultGroup.getSeries().iterator());

            if (group.getType() == MetricType.POINT) {

                final List<Point> data= group.getDataAs(Point.class);

                int x,y;
                x=0;
                y=0;

                for (final Point p :data){

                    String[] R;
                    Map<String,Integer> map = new TreeMap<String, Integer>();

                    // Timestamp
                    double t = p.getTimestamp();
                    double lastime = t;
                    //System.out.println(t);
                    if (listxValues.contains(t)){
                        x=listxValues.indexOf(t);
                    }else{
                        listxValues.add(t);
                        x=listxValues.indexOf(t);
                    }

                    double v = p.getValue();
                    Map<String, SortedSet<String>> tags = series.getTags();
                    String k="";
                    if (series.getKeys().size() == 1) {
                        k = series.getKeys().iterator().next();
                    }
                    TreeMap val = null;
                    for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {


                        Double f=null;
                        final SortedSet<String> values = pair.getValue();
                        if (values.size() != 1) {
                            continue;
                        }

                        String K = pair.getKey();

                        if (k.equals("orfees") && K.equals("f")){
                            String text = values.iterator().next();
                            f = Double.parseDouble(text);

                        }
                        if (k.equals("nrh") && K.equals("coor")){
                            String text = values.iterator().next();
                            f = Double.parseDouble(text);

                        }
                        if (listyValues.contains(f)){

                            y=listyValues.indexOf(f);
                        }else{

                            listyValues.add(f);
                            y=listyValues.indexOf(f);
                        }
                        if(k.equals("nrh")||k.equals("orfees")) {
                            //listzValues.addToInnerArray(t, f, v);
                            //System.out.println("TreeMap");
                            if(f==null)f=0D;
                            //System.out.println(f);
                            //System.out.println(t);
                            //System.out.println(v);
                            val=zzz.get(t);
                            if(val!=null){
                                val.put(f,v);
                            }else{
                                val = new TreeMap<Double, Double>();

                            }

                            //TreeMap<Double, Double> val = new TreeMap<Double, Double>();
                            val.put(f,v);
                            //System.out.println(val);

                            // Do what you want to do with val
                            //System.out.println(t);
                            zzz.put(t, val);
                            //arval.add(v);

                            //System.out.println(zzz);
                        }

                    }
                    if (maxz == null) {
                        maxz = v;
                    }
                    if (minz == null) {
                        minz = v;
                    }
                    if(v > maxz){
                        maxz = v;
                    }
                    if(v < minz){
                        minz = v;
                    }
                    size = val.size();
                }


            }


        }
        //System.out.println("arrayList");
        //System.out.println(listxValues);
        //System.out.println(listyValues);
        //System.out.println(listzValues);
        //System.out.println("array");
        //System.out.println(zzz);

        //Double[] xValues = listxValues.toArray(new Double[listxValues.size()]);
        //Double[] yValues = listyValues.toArray(new Double[listyValues.size()]);
        //Double[][] zValues = listzValues.stream().map(u -> u.toArray(new Double[0])).toArray(Double[][]::new);
       // double[][] z =  ArrayUtils.toPrimitive(d);


        //double[] flat = ArrayUtil.flattenDoubleArray(zValues);
        int[] shape = {zzz.size(),(zzz.get(lasttime)).size()};	//Array shape here




        //INDArray myArr = Nd4j.create(flat,shape,'c');

        //int min = Collections.min(listzValues);
        //int max = Collections.max(listzValues);

        //double[][] data = HeatMap.generateSinCosData(200);
        //boolean useGraphicsYAxis = true;

        // you can use a pre-defined gradient:
        //HeatMap panel = new HeatMap(z, useGraphicsYAxis, Gradient.GRADIENT_BLACK_TO_WHITE);

        //HeatChart map = new HeatChart(zValues);

        // Step 2: Customise the chart.
        //map.setTitle("This is my heat chart title");
        //map.setXAxisLabel("X Axis");
        //map.setYAxisLabel("Y Axis");

        // Step 3: Output the chart to a file.
        //map.saveToFile(new File("java-heat-chart.png"));
        //Double maxx = Collections.max(zzz);
        double range = maxz.doubleValue() - minz.doubleValue();
        /**
        Collection<TreeMap<Double,Double>> values =(Collection<TreeMap<Double,Double>>) zzz.values();
        TreeMap<Double,Double>[] targetArray =(TreeMap<Double, Double>[]) values.toArray();
        for(TreeMap<Double,Double> tm: targetArray){
            Double[] da =(Double[]) tm.values().toArray();


        }
         */
        Set set = zzz.entrySet();
        Iterator i = set.iterator();
        BufferedImage img = new BufferedImage(zzz.size(), size, BufferedImage.TYPE_INT_ARGB);
        byte[] data;
        int h=0;
        int n=0;
        while(i.hasNext()) {
         Map.Entry<Double,TreeMap> me = (Map.Entry) i.next();
         TreeMap tmv = (TreeMap)   me.getValue();
         Double[] la =(Double[]) tmv.values().toArray();
         System.out.println(la);
         Set jset = (me.getValue()).entrySet();
         Iterator j = jset.iterator();
         //System.out.print(me.getKey() + ": ");
         Gradient G = new Gradient();
         Color[] colors= G.GRADIENT_BLUE_TO_RED;
         //System.out.println(me.getValue());


        /**
         while(j.hasNext()) {
             Map.Entry<Double,Double> jme = (Map.Entry)j.next();
             //System.out.print(jme.getKey() + ": ");
                //System.out.println(jme.getValue());
                double zv = ((jme.getValue()-minz.doubleValue())/(maxz.doubleValue()-minz.doubleValue()));
                //System.out.println(zv);
                //double zv = Math.abs(jme.getValue()-minz.doubleValue())/3;

                double ZV = zv * 256;
                //System.out.println(ZV);
                int iz = Math.toIntExact(Math.round(ZV));
                //System.out.println(iz);
                iz = Math.min(iz,255);

                //int colorIndex = (int) Math.floor(norm * (colors.length - 1));
                //bufferedGraphics.setColor(colors[dataColorIndices[x][y]]);
                //int r =  iz;// red component 0...255
                //int f = r;// green component 0...255
                //int b = r;// blue component 0...255
                ///int col = (r << 16) | (n << 8) | b;
                ///System.out.println(col);
                //img.setRGB(h, n, col);
                img.setRGB(h, n,colors[iz].getRGB());

                n=n+1;
            }
            */
            h=h+1;

        }
        //ByteArrayInputStream stream = new ByteArrayInputStream(data);
        //img = read(stream);


        //System.out.println(minz);
        //System.out.println(maxz);
        //BufferedImage img = map.getChartImage(true);
        /**
        for (int h = 0;h <shape[0];h++  ){
            for (int n = 0;n <shape[1];n++  ) {

                double zv = (zValues[h][n]-minz.doubleValue())/(maxz.doubleValue()-minz.doubleValue())*255;
                int iz = Math.toIntExact(Math.round(zv));
                int r =  iz;// red component 0...255
                int f = r;// green component 0...255
                int b = r;// blue component 0...255
                int col = (r << 16) | (n << 8) | b;
                img.setRGB(h, n, col);
            }
        }
        */

        //init();
        //BufferedImage img = panel.getImage();
        return img;

    }
    /**
    @Override
    public  void init() {
        // Define a function to plot
        Mapper mapper = new Mapper() {
            @Override
            public double f(double x, double y) {
                return x * Math.sin(x * y);
            }
        };

        // Define range and precision for the function to plot
        Range range = new Range(-3, 3);
        int steps = 80;

        // Create the object to represent the function over the given range.
        final Shape surface = Builder.buildOrthonormal(new OrthonormalGrid(range, steps, range, steps), mapper);
        surface.setColorMapper(new ColorMapper(new ColorMapRainbow(), surface.getBounds().getZmin(), surface.getBounds().getZmax(), new Color(1, 1, 1, .5f)));
        surface.setFaceDisplayed(true);
        surface.setWireframeDisplayed(false);

        // Create a chart
        chart = AWTChartComponentFactory.chart(Quality.Advanced, getCanvasType());
        chart.getScene().getGraph().add(surface);
    }
    */


}

