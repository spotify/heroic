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

import java.awt.BasicStroke;
import java.awt.Color;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;

public class HeatmapUtil {

    private static final List<Color> COLORS = new ArrayList<>();

    static {
        COLORS.add(Color.BLUE);
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

                /**
                 * Utility method called by createDataset().
                 *
                 * @param data  the data array.
                 * @param c  the column.
                 * @param r  the row.
                 * @param value  the value.
                 *

                private static void setValue(double[][] data,
                int c, int r, double value) {

                    data[0][(r - 8) * 60 + c] = c;
                    data[1][(r - 8) * 60 + c] = r;
                    data[2][(r - 8) * 60 + c] = value;

                }
                */

                final double[][] DATA = null;

                for (final Point p :data){

                    String[] R;
                    Map<String,Integer> map = new TreeMap<String, Integer>();
                    int c = new BigDecimal(p.getTimestamp()).intValue();
                    System.out.print("getTimestamp");
                    System.out.println(  p.getTimestamp());
                    System.out.println(c);
                    System.out.print("getValue")  ;
                    Double value = p.getValue();
                    System.out.println(value);
                    Map<String, SortedSet<String>> tags = series.getTags();
                    //g.writeStringField("timestamp", strLong);
                    //g.writeStringField("value",strDouble );
                    //writeKey(g, series.getKeys());
                    //writeTags(g, common, series.getTags());


                    for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {



                        final SortedSet<String> values = pair.getValue();
                        System.out.print("values");
                        System.out.println(values);
                        System.out.print("getKey");
                        System.out.println(pair.getKey());
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
                        //map.put(pair.getKey(),new BigDecimal (values.iterator().next()).intValue());
                    }

                    //int r = map.get("coor");

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

