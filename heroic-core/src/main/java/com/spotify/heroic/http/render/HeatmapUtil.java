package com.spotify.heroic.http.render;

/**
 * Created by lucile on 09/05/17.
 */

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;


import java.awt.Color;
import java.awt.image.BufferedImage;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.ArrayList;
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

public class HeatmapUtil {
    //extends AbstractAnalysis


    protected double[] xValues = null;
    protected double[] yValues = null;

    protected double[][] zValues = null;



    public  BufferedImage createChart(
        final List<ShardedResultGroup> groups, final String title, Map<String, String> highlight,
        Double threshold, int height
    ) {

        TwoDimentionalArrayList<Double> listzValues = new TwoDimentionalArrayList();
        List<Double> listxValues = new ArrayList();
        List<Double> listyValues = new ArrayList();


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
                            listzValues.addToInnerArray(x, y, v);
                        }

                    }



                }

            }


        }
        //System.out.println("arrayList");
        //System.out.println(listxValues);
        //System.out.println(listyValues);
        //System.out.println(listzValues);
        System.out.println("array");

        Double[] xValues = listxValues.toArray(new Double[listxValues.size()]);
        Double[] yValues = listyValues.toArray(new Double[listyValues.size()]);
        Double[][] zValues = listzValues.stream().map(u -> u.toArray(new Double[0])).toArray(Double[][]::new);

        double[] flat = ArrayUtil.flattenDoubleArray(zValues);
        int[] shape = {zValues.length,zValues[0].length};	//Array shape here
        INDArray myArr = Nd4j.create(flat,shape,'c');





        HeatChart map = new HeatChart(zValues);

        // Step 2: Customise the chart.
        map.setTitle("This is my heat chart title");
        map.setXAxisLabel("X Axis");
        map.setYAxisLabel("Y Axis");

        // Step 3: Output the chart to a file.
        //map.saveToFile(new File("java-heat-chart.png"));

        BufferedImage img = map.getChartImage(true);



        //init();

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

