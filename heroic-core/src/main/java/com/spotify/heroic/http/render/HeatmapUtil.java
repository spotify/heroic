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
import org.jfree.data.general.HeatMapUtilities;
import org.jfree.data.general.HeatMapDataset;

import org.jfree.data.general.DefaultHeatMapDataset;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYBlockRenderer;
import org.jfree.data.xy.DefaultXYZDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.RectangleAnchor;
import org.jfree.chart.renderer.LookupPaintScale;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.ArrayList;



public class HeatmapUtil {

    protected static XYDataset dataset;
    protected static NumberAxis xAxis = new NumberAxis("X");
    protected static NumberAxis yAxis = new NumberAxis("Y");
    protected static String dataSetName = "";

    static LookupPaintScale paintScale = null;

    protected static XYBlockRenderer renderer = null; 

    protected double[] xValues = null;
    protected double[] yValues = null;
    private static final List<Color> COLORS = new ArrayList<>();
    protected double[][] zValues = null;
    public static final int PALETTE_BLUE_RED = 0;

    public static final int PALETTE_RED = 1;
    protected static double lowerZBound = 0;
    public static final int PALETTE_BLUE = 2;
    protected static double upperZBound = 0;
    public static final int PALETTE_GRAYSCALE = 3;
    public static final int PALETTE_WHITE_BLUE = 4;
    //This palette starts with blue at 0 and scales through the values.  Any negative values get the same red color
    public static final int PALETTE_POSITIVE_WHITE_BLUE_NEGATIVE_BLACK_RED = 5;
    public static final int DEFAULT_PALETTE = PALETTE_BLUE_RED;

    protected static int palette = DEFAULT_PALETTE;

    public static final int DISTINCT_PALETTE_VALUES = 100;

    public static void setPaintScale(LookupPaintScale newPaintScale)
    {
        paintScale = newPaintScale;

        renderer.setPaintScale(newPaintScale);
    }

    protected static void addValuesToPaintScale(LookupPaintScale paintScale, double lowerBound, double upperBound,
                                                Color lowColor, Color highColor)
    {
        int distinctValues = DISTINCT_PALETTE_VALUES;

        if (upperBound <= lowerBound)
            upperBound = lowerBound + .0001;
        double increment = (upperBound - lowerBound) / distinctValues;

        int redDiff = highColor.getRed() - lowColor.getRed();
        int greenDiff = highColor.getGreen() - lowColor.getGreen();
        int blueDiff = highColor.getBlue() - lowColor.getBlue();
        double redIncrement = (redDiff / distinctValues);
        double greenIncrement = (greenDiff / distinctValues);
        double blueIncrement = (blueDiff / distinctValues);
        for (int i=0; i<distinctValues; i++)
        {
            int r = (int) (lowColor.getRed() + (i * redIncrement));
            int g = (int) (lowColor.getGreen() + (i * greenIncrement));
            int b = (int) (lowColor.getBlue() + (i * blueIncrement));
            Color incrementColor = new Color(r,g,b);
            double incrementStart = lowerBound + (i * increment);
            paintScale.add(incrementStart, incrementColor);

        }
    }

    static {
        COLORS.add(Color.BLUE);
    }
    public static LookupPaintScale createPaintScale(double lowerBound,
                                       double upperBound,
                                       Color lowColor, Color highColor)
    {
        //prevent rounding errors that make highest value undefine
        LookupPaintScale result = new LookupPaintScale(lowerBound, upperBound+0.01, lowColor);
        addValuesToPaintScale(result, lowerBound, upperBound, lowColor, highColor);
        return result;
    }
    public static LookupPaintScale createPaintScale(int palette)
    {
        System.out.println("palette");
        LookupPaintScale result;
        switch (palette)
        {
            case PALETTE_BLUE_RED:
                result = createPaintScale(lowerZBound, upperZBound, Color.BLUE, Color.RED);
                break;
            case PALETTE_RED:
                result = createPaintScale(lowerZBound, upperZBound, new Color(70,5,5), new Color(255,5,5));
                break;
            case PALETTE_BLUE:
                result = createPaintScale(lowerZBound, upperZBound, new Color(5,5,70), new Color(5,5,255));
                break;
            case PALETTE_GRAYSCALE:
                result = createPaintScale(lowerZBound, upperZBound, Color.WHITE, new Color(5,5,5));
                break;
            case PALETTE_WHITE_BLUE:
                result = createPaintScale(lowerZBound, upperZBound, Color.WHITE, Color.BLUE);
                break;
            case PALETTE_POSITIVE_WHITE_BLUE_NEGATIVE_BLACK_RED:
                result = new LookupPaintScale(lowerZBound, upperZBound+0.1, Color.RED);
                addValuesToPaintScale(result, 0, upperZBound, Color.WHITE, Color.BLUE);
                addValuesToPaintScale( result, -upperZBound-0.000001, -0.0000001, Color.BLUE, Color.RED);
                break;
            default:
                result = createPaintScale(lowerZBound, upperZBound, Color.WHITE, new Color(5,5,5));
                break;
        }
        System.out.println("end palette");
        System.out.println( result);
        return result;
    }

    public static BufferedImage createChart(
        final List<ShardedResultGroup> groups, final String title, Map<String, String> highlight,
        Double threshold, int height
    ) {
        final DefaultXYZDataset dataset = new DefaultXYZDataset();
        TwoDimentionalArrayList<Double> listzValues = new TwoDimentionalArrayList();
        List<Double> listxValues = new ArrayList();
        List<Double> listyValues = new ArrayList();
        LookupPaintScale paintScale = null;

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

                    Double v = p.getValue();
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
        //System.out.println("array");
        Double[] xValues = listxValues.toArray(new Double[listxValues.size()]);
        Double[] yValues = listyValues.toArray(new Double[listyValues.size()]);
        Double[][] zValues = listzValues.stream().map(u -> u.toArray(new Double[0])).toArray(Double[][]::new);
        System.out.println("array");
        System.out.println(xValues);
        System.out.println(yValues);
        System.out.println(zValues);

        BufferedImage img = new BufferedImage(zValues[0].length,zValues.length , BufferedImage.TYPE_INT_RGB);
        r = img.getRaster();
        System.out.println(yValues.length);//65
        System.out.println(xValues.length);//2030
        System.out.println(zValues.length);//2030 --> x
        System.out.println(zValues[0].length);//65 --> y

        for(int y=0;y<zValues.length;y++){
            // 65
            // System.out.println(y);
            //System.out.print("y : ");
            //System.out.println(y);
            for(int x=0;x<zValues[0].length;x++){
                //System.out.print("x : ");
                //System.out.println(x);

                Integer z =Double.valueOf( zValues[y][x]*1000).intValue();
                System.out.print("z : ");
                System.out.println(z);
                int Pixel= z<<16 | z << 8 | z;
                //System.out.print("img : ");
                img.setRGB(x, y,Pixel);
                //System.out.println("ok ");
            }

        }
        return img;
        //ImageIO.write(b, "Doublearray", new File("Doublearray.jpg"));
        //System.out.println("end");
        /**
        try {

            byte[] imageInByte;
            //BufferedImage originalImage = ImageIO.read(new File("c:/darksouls.jpg"));

            // convert BufferedImage to byte array
            //ByteArrayOutputStream baos = new ByteArrayOutputStream();
            //ImageIO.write(originalImage, "jpg", baos);
            //baos.flush();

            imageInByte = baos.toByteArray();
            baos.close();
            imageInByte = DATA;
            // convert byte array back to BufferedImage
            InputStream in = new ByteArrayInputStream(imageInByte);
            BufferedImage bImageFromConvert = ImageIO.read(in);

            ImageIO.write(bImageFromConvert, "jpg", new File("c:/new-darksouls.jpg"));

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }



        final JFreeChart chart = setData( xValues, yValues,zValues);
        return chart;
         */
    }

    private static JFreeChart setData(Double[] xValues, Double[] yValues, Double[][] zValues)
    {
        int palette = PALETTE_BLUE_RED;

        LookupPaintScale paintScale = null;


        renderer = new XYBlockRenderer();

        Double minZValue = Double.MAX_VALUE;
        Double maxZValue = Double.MIN_VALUE;
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
                if (yValues[j]==null) {
                    data[1][cellIndex] = 0;
                }else{
                    data[1][cellIndex] = yValues[j];
                }
                data[2][cellIndex]= zValues[i][j];

                //keep track of lowest/highest z values
                minZValue = Math.min(zValues[i][j], minZValue);
                maxZValue = Math.max(zValues[i][j], maxZValue);

            }
        }
        Double lowerZBound = Rounder.round(minZValue,3);
        Double upperZBound = Rounder.round(maxZValue,3);
        //if (lowerZBound == upperZBound)
        //    upperZBound += .0001;
        //_log.debug("low,high values: " + lowerZBound + ", " + upperZBound);
        theDataset.addSeries("Range: " + lowerZBound + "-" + upperZBound,data);

        DefaultXYZDataset dataset = theDataset;
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

        //if (XYPlot.getPlot() != null)
        //{
         //   ((XYPlot) XYPlot.getPlot()).setDataset(dataset);
         //   ((XYPlot) XYPlot.getPlot()).setRenderer(renderer);

            //invalidate();
        //    return;
        //}
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

        //init(chart);
        return chart;
    }

}

