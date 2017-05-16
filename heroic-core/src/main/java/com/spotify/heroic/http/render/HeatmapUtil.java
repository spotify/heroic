package com.spotify.heroic.http.render;

/**
 * Created by lucile on 09/05/17.
 */

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.DeviationRenderer;
import org.jfree.chart.renderer.xy.XYBlockRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.DefaultXYZDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.data.xy.YIntervalSeriesCollection;
import org.jfree.ui.RectangleAnchor;

import java.awt.BasicStroke;
import java.awt.Color;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.ArrayList;



public class HeatmapUtil {

    private static final List<Color> COLORS = new ArrayList<>();

    static {
        COLORS.add(Color.BLUE);
    }

    public void addToInnerArray(int index, T element) {
        while (index >= this.size()) {
            this.add(new ArrayList<T>());
        }
        this.get(index).add(element);
    }

    public void addToInnerArray(int index, int index2, T element) {
        while (index >= this.size()) {
            this.add(new ArrayList<T>());
        }

        ArrayList<T> inner = this.get(index);
        while (index2 >= inner.size()) {
            inner.add(null);
        }

        inner.set(index2, element);
    }

    public static JFreeChart createChart(
        final List<ShardedResultGroup> groups, final String title, Map<String, String> highlight,
        Double threshold, int height
    ) {
        final XYLineAndShapeRenderer lineAndShapeRenderer = new XYLineAndShapeRenderer(true, true);
        final DeviationRenderer intervalRenderer = new DeviationRenderer();

        final XYSeriesCollection regularData = new XYSeriesCollection();
        final YIntervalSeriesCollection intervalData = new YIntervalSeriesCollection();
        final DefaultXYZDataset dataset = new DefaultXYZDataset();
        int lineAndShapeCount = 0;
        int intervalCount = 0;

        for (final ShardedResultGroup resultGroup : groups) {
            final MetricCollection group = resultGroup.getMetrics();
            final SeriesValues series = SeriesValues.fromSeries(resultGroup.getSeries().iterator());

            if (group.getType() == MetricType.POINT) {

                final List<Point> data= group.getDataAs(Point.class);




                ArrayList<double> listzValues = new TwoDimentionalArrayList();
                ArrayList<double> listxValues = new ArrayList<double>();
                ArrayList<double> listyValues = new ArrayList<double>();



                final double[][] zValues = null;
                final double[] xValues = null;
                final double[] yValues = null;

                for (final Point p :data){

                    String[] R;
                    Map<String,Integer> map = new TreeMap<String, Integer>();
                    double t = p.getTimestamp();
                    listxValues.add(t);
                    //System.out.print("getTimestamp");
                    //System.out.println(  p.getTimestamp());
                    //System.out.println(c);
                    //System.out.print("getValue")  ;
                    Double v = p.getValue();



                    //System.out.println(value);
                    Map<String, SortedSet<String>> tags = series.getTags();
                    //g.writeStringField("timestamp", strLong);
                    //g.writeStringField("value",strDouble );
                    //writeKey(g, series.getKeys());
                    //writeTags(g, common, series.getTags());


                    for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {


                        String f="";
                        final SortedSet<String> values = pair.getValue();
                        //System.out.print("values");
                        //System.out.println(values);
                        //System.out.print("getKey");
                        //System.out.println(pair.getKey());
                        if (values.size() != 1) {
                            continue;
                        }


                        if(pair.getKey()=="coor") {
                            //byte r = Byte.valueOf(values.iterator().next() );
                            //map.put(pair.getKey(), new BigDecimal(values.iterator().next()).intValue());
                            //system.out.println(  val);
                            //int r = new BigDecimal(values.iterator().next() ).intValue();
                            //DATA[c][] = Byte.valueOf(value);
                        }
                        String K = pair.getKey();
                        if (k.equals("orfees") && K.equals("f")){
                            //System.out.println("condition ok ");
                            //g.writeString( values.iterator().next());
                            f = values.iterator().next();
                            listzValues.addToInnerArray(t, f, v);
                        }


                        if (k.equals("nrh") && K.equals("coor")){
                            //System.out.println("condition ok ");
                            //g.writeString( values.iterator().next());
                            f = values.iterator().next();
                            listzValues.addToInnerArray(t, f, v);
                        }


                        //map.put(pair.getKey(),new BigDecimal (values.iterator().next()).intValue());
                    }

                    //int r = map.get("coor");
                    System.out.println("list");
                    System.out.println(listzValues);

                    //DATA[c][r] = value;

                }

                //System.out.println(DATA);
                //System.out.println(DATA);
                //final XYSeries series = new XYSeries(resultGroup.getMetrics().toString());

                //dataset.addSeries("f", DATA);

                /**
                 for (final Point d : data) {
                 series.add(d.getTimestamp(), d.getValue());
                 }

                 lineAndShapeRenderer.setSeriesPaint(lineAndShapeCount, Color.BLUE);
                 lineAndShapeRenderer.setSeriesShapesVisible(lineAndShapeCount, false);
                 lineAndShapeRenderer.setSeriesStroke(lineAndShapeCount, new BasicStroke(2.0f));
                 regularData.addSeries(series);
                 */
                ++lineAndShapeCount;
            }


            /**
             intervalRenderer.setSeriesPaint(intervalCount, Color.GREEN);
             intervalRenderer.setSeriesStroke(intervalCount, new BasicStroke(2.0f));
             intervalRenderer.setSeriesFillPaint(intervalCount, new Color(200, 255, 200));
             intervalRenderer.setSeriesShapesVisible(intervalCount, false);
             intervalData.addSeries(series);
             ++intervalCount;
             */

        }
        //try {

        //    byte[] imageInByte;
        //BufferedImage originalImage = ImageIO.read(new File("c:/darksouls.jpg"));

        // convert BufferedImage to byte array
        //ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //ImageIO.write(originalImage, "jpg", baos);
        //baos.flush();
        //imageInByte = baos.toByteArray();
        //baos.close();
        //imageInByte = DATA
        // convert byte array back to BufferedImage
        //InputStream in = new ByteArrayInputStream(imageInByte);
        //BufferedImage bImageFromConvert = ImageIO.read(in);

        //ImageIO.write(bImageFromConvert, "jpg", new File("c:/new-darksouls.jpg"));

        //} catch (IOException e) {
        //    System.out.println(e.getMessage());
        //}


        final JFreeChart chart =
            buildChart(title, regularData, intervalData, lineAndShapeRenderer, intervalRenderer, dataset);

        chart.setAntiAlias(true);
        chart.setBackgroundPaint(Color.WHITE);

        final XYPlot plot = chart.getXYPlot();

        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.BLACK);
        plot.setRangeGridlinePaint(Color.BLACK);

        if (threshold != null) {
            final ValueMarker marker = new ValueMarker(threshold, Color.RED,
                new BasicStroke(Math.max(Math.min(height / 20, 6), 1)), Color.RED, null, 0.5f);
            plot.addRangeMarker(marker);
        }

        plot.setRenderer(lineAndShapeRenderer);

        // final DateAxis rangeAxis = (DateAxis) plot.getRangeAxis();
        // rangeAxis.setStandardTickUnits(DateAxis.createStandardDateTickUnits());

        return chart;
    }

    public void setData(double[] xValues, double[] yValues, double[][] zValues)
    {
        this.xValues = xValues;
        this.yValues = yValues;
        this.zValues = zValues;

        double minZValue = Double.MAX_VALUE;
        double maxZValue = Double.MIN_VALUE;
        int width = xValues.length;
        int height = yValues.length;
        int numCells = width * height;

        if (zValues.length != width || zValues[0].length != height)
            throw new RuntimeException("PanelWithHeatMap: wrong number of z values for x and y values (" +
                zValues.length + " vs. " + width + ", " + zValues[0].length + " vs. " + height +
                ", x/y first, z second)");
        DefaultXYZDataset theDataset = new DefaultXYZDataset();
        double[][] data = new double[3][numCells];
        for(int j=0; j<height; j++){
            for(int i=0; i<width; i++)
            {
                int cellIndex = (j * width) + i;
                data[0][cellIndex]= xValues[i];
                data[1][cellIndex]= yValues[j];
                data[2][cellIndex]= zValues[i][j];
                //keep track of lowest/highest z values
                minZValue = Math.min(zValues[i][j], minZValue);
                maxZValue = Math.max(zValues[i][j], maxZValue);
            }
        }
        lowerZBound = Rounder.round(minZValue,3);
        upperZBound = Rounder.round(maxZValue,3);
        if (lowerZBound == upperZBound)
            upperZBound += .0001;
        _log.debug("low,high values: " + lowerZBound + ", " + upperZBound);
        theDataset.addSeries("Range: " + lowerZBound + "-" + upperZBound,data);

        dataset = theDataset;
        if (renderer == null)
        {
            renderer = new XYBlockRenderer();
        }

        if (paintScale == null)
        {
            setPaintScale(createPaintScale(palette));
        }
        //This is necessary to get everything to line up
        renderer.setBlockAnchor(RectangleAnchor.BOTTOM);

        if (getPlot() != null)
        {
            ((XYPlot) getPlot()).setDataset(dataset);
            ((XYPlot) getPlot()).setRenderer(renderer);

            invalidate();
            return;
        }
        XYPlot plot = new XYPlot(dataset, xAxis, yAxis, renderer);

        JFreeChart chart = new JFreeChart(dataSetName,JFreeChart.DEFAULT_TITLE_FONT,plot,true);

        //        chart.addLegend(new LegendTitle(renderer));
        //        PaintScaleLegend legend = new PaintScaleLegend(paintScale, xAxis);
        //        chart.addLegend(legend);


        //        LegendItemCollection c1 = new LegendItemCollection();
        //
        //        LegendItem item1 = new LegendItem("Label", "Description",
        //                "ToolTip", "URL", true,
        //                new Rectangle2D.Double(1.0, 2.0, 3.0, 4.0), true, Color.red,
        //                true, Color.blue, new BasicStroke(1.2f), true,
        //                new Line2D.Double(1.0, 2.0, 3.0, 4.0),
        //                new BasicStroke(2.1f), Color.green);
        //        LegendItem item2 = new LegendItem("Label", "Description",
        //                "ToolTip", "URL", true,
        //                new Rectangle2D.Double(1.0, 2.0, 3.0, 4.0),
        //                true, Color.red, true, Color.blue, new BasicStroke(1.2f), true,
        //                new Line2D.Double(1.0, 2.0, 3.0, 4.0), new BasicStroke(2.1f),
        //                Color.green);
        //        c1.add(item1);
        //
        //        chart.getLegend().setSources(new LegendItemSource[]{renderer});

        init(chart);
    }

    private static JFreeChart buildChart(
        final String title, final XYDataset lineAndShape, final XYDataset interval,
        final XYItemRenderer lineAndShapeRenderer, final XYItemRenderer intervalRenderer,XYDataset dataset
    ) {
        final ValueAxis timeAxis = new DateAxis();
        timeAxis.setLowerMargin(0.02);
        timeAxis.setUpperMargin(0.02);


        final NumberAxis valueAxis = new NumberAxis();
        valueAxis.setAutoRangeIncludesZero(false);

        XYBlockRenderer renderer = new XYBlockRenderer();

        final XYPlot plot = new XYPlot(dataset,timeAxis , valueAxis, renderer);

        //final XYPlot plot = new XYPlot();

        plot.setDomainAxis(0, timeAxis);
        plot.setRangeAxis(0, valueAxis);

        plot.setDataset(0, lineAndShape);
        plot.setRenderer(0, lineAndShapeRenderer);

        plot.setDomainAxis(1, timeAxis);
        plot.setRangeAxis(1, valueAxis);

        plot.setDataset(1, interval);
        plot.setRenderer(1, intervalRenderer);

        return new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, false);
    }
}

