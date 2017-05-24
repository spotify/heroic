/*  
 *  Copyright 2010 Tom Castle (www.tc33.org)
 *  Licensed under GNU Lesser General Public License
 * 
 *  This file is part of JHeatChart - the heat maps charting api for Java.
 *
 *  JHeatChart is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published 
 *  by the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  JHeatChart is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with JHeatChart.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.spotify.heroic.http.render;


import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Iterator;

import javax.imageio.*;
import javax.imageio.stream.FileImageOutputStream;

/**
 * The <code>HeatChart</code> class describes a chart which can display 
 * 3-dimensions of values - x,y and z, where x and y are the usual 2-dimensional
 * axis and z is portrayed by colour intensity. Heat charts are sometimes known 
 * as heat maps. 
 *
 * <p>
 * Use of this chart would typically involve 3 steps:
 * <ol>
 * <li>Construction of a new instance, providing the necessary z-values.</li>
 * <li>Configure the visual settings.</li>
 * <li>A call to either <code>getChartImage()</code> or <code>saveToFile(String)</code>.</li>
 * </ol>
 *
 * <h3>Instantiation</h3>
 * <p>
 * Construction of a new <code>HeatChart</code> instance is through its one
 * constructor which takes a 2-dimensional array of <tt>doubles</tt> which 
 * should contain the z-values for the chart. Consider this array to be 
 * the grid of values which will instead be represented as colours in the chart.
 *
 * <p>
 * Setting of the x-values and y-values which are displayed along the 
 * appropriate axis is optional, and by default will simply display the values 
 * 0 to n-1, where n is the number of rows or columns. Otherwise, the x/y axis 
 * values can be set with the <code>setXValues</code> and <code>setYValues
 * </code> methods. Both methods are overridden with two forms:
 *
 * <h4>Object axis values</h4>
 *
 * <p>
 * The simplist way to set the axis values is to use the methods which take an
 * array of Object[]. This array must have the same length as the number of 
 * columns for setXValues and same as the number of rows for setYValues. The 
 * string representation of the objects will then be used as the axis values.
 *
 * <h4>Offset and Interval</h4>
 *
 * <p>
 * This is convenient way of defining numerical values along the axis. One of 
 * the two methods takes an interval and an offset for either the 
 * x or y axis. These parameters supply the necessary information to describe 
 * the values based upon the z-value indexes. The quantity of x-values and 
 * y-values is already known from the lengths of the z-values array dimensions. 
 * Then the offset parameters indicate what the first value will be, with the
 * intervals providing the increment from one column or row to the next.
 *
 * <p>
 * <strong>Consider an example:</strong>
 * <blockquote><pre>
 * double[][] zValues = new double[][]{
 * 		{1.2, 1.3, 1.5},
 * 		{1.0, 1.1, 1.6},
 * 		{0.7, 0.9, 1.3}
 * };
 *
 * double xOffset = 1.0;
 * double yOffset = 0.0;
 * double xInterval = 1.0;
 * double yInterval = 2.0;
 *
 * chart.setXValues(xOffset, xInterval);
 * chart.setYValues(yOffset, yInterval);
 * </pre></blockquote>
 *
 * <p>In this example, the z-values range from 0.7 to 1.6. The x-values range 
 * from the xOffset value 1.0 to 4.0, which is calculated as the number of x-values 
 * multiplied by the xInterval, shifted by the xOffset of 1.0. The y-values are 
 * calculated in the same way to give a range of values from 0.0 to 6.0. 
 *
 * <h3>Configuration</h3>
 * <p>
 * This step is optional. By default the heat chart will be generated without a 
 * title or labels on the axis, and the colouring of the heat map will be in 
 * grayscale. A large range of configuration options are available to customise
 * the chart. All customisations are available through simple accessor methods.
 * See the javadoc of each of the methods for more information.
 *
 * <h3>Output</h3>
 * <p>
 * The generated heat chart can be obtained in two forms, using the following 
 * methods:
 * <ul>
 * <li><strong>getChartImage()</strong> - The chart will be returned as a 
 * <code>BufferedImage</code> object that can be used in any number of ways, 
 * most notably it can be inserted into a Swing component, for use in a GUI 
 * application.</li>
 * <li><strong>saveToFile(File)</strong> - The chart will be saved to the file 
 * system at the file location specified as a parameter. The image format that  
 * the image will be saved in is derived from the extension of the file name.</li>
 * </ul>
 *
 * <strong>Note:</strong> The chart image will not actually be created until 
 * either saveToFile(File) or getChartImage() are called, and will be 
 * regenerated on each successive call.
 */
public class HeatChart {

    /**
     * A basic logarithmic scale value of 0.3.
     */
    public static final double SCALE_LOGARITHMIC = 0.3;

    /**
     * The linear scale value of 1.0.
     */
    public static final double SCALE_LINEAR = 1.0;

    /**
     * A basic exponential scale value of 3.0.
     */
    public static final double SCALE_EXPONENTIAL = 3;

    // x, y, z data values.
    private double[][] zValues;
    private Object[] xValues;
    private Object[] yValues;

    private boolean xValuesHorizontal;
    private boolean yValuesHorizontal;

    // General chart settings.
    private Dimension cellSize;
    private Dimension chartSize;
    private int margin;
    private Color backgroundColour;

    // Title settings.
    private String title;
    private Font titleFont;
    private Color titleColour;
    private Dimension titleSize;
    private int titleAscent;

    // Axis settings.
    private int axisThickness;
    private Color axisColour;
    private Font axisLabelsFont;
    private Color axisLabelColour;
    private String xAxisLabel;
    private String yAxisLabel;
    private Color axisValuesColour;
    private Font axisValuesFont; // The font size will be considered the maximum font size - it may be smaller if needed to fit in.
    private int xAxisValuesFrequency;
    private int yAxisValuesFrequency;
    private boolean showXAxisValues;
    private boolean showYAxisValues;

    // Generated axis properties.
    private int xAxisValuesHeight;
    private int xAxisValuesWidthMax;

    private int yAxisValuesHeight;
    private int yAxisValuesAscent;
    private int yAxisValuesWidthMax;

    private Dimension xAxisLabelSize;
    private int xAxisLabelDescent;

    private Dimension yAxisLabelSize;
    private int yAxisLabelAscent;

    // Heat map colour settings.
    private Color highValueColour;
    private Color lowValueColour;

    // How many RGB steps there are between the high and low colours.
    private int colourValueDistance;

    private double lowValue;
    private double highValue;

    // Key co-ordinate positions.
    private Point heatMapTL;
    private Point heatMapBR;
    private Point heatMapC;

    // Heat map dimensions.
    private Dimension heatMapSize;

    // Control variable for mapping z-values to colours.
    private double colourScale;

    /**
     * Constructs a heatmap for the given z-values against x/y-values that by
     * default will be the values 0 to n-1, where n is the number of columns or
     * rows.
     *
     * @param zValues the z-values, where each element is a row of z-values
     * in the resultant heat chart.
     */
    public HeatChart(double[][] zValues) {
        this(zValues, min(zValues), max(zValues));
    }

    /**
     * Constructs a heatmap for the given z-values against x/y-values that by
     * default will be the values 0 to n-1, where n is the number of columns or
     * rows.
     *
     * @param zValues the z-values, where each element is a row of z-values
     * in the resultant heat chart.
     * @param low the minimum possible value, which may or may not appear in the
     * z-values.
     * @param high the maximum possible value, which may or may not appear in
     * the z-values.
     */
    public HeatChart(double[][] zValues, double low, double high) {
        this.zValues = zValues;
        this.lowValue = low;
        this.highValue = high;

        // Default x/y-value settings.
        setXValues(0, 1);
        setYValues(0, 1);

        // Default chart settings.
        this.cellSize = new Dimension(20, 20);
        this.margin = 20;
        this.backgroundColour = Color.WHITE;

        // Default title settings.
        this.title = null;
        this.titleFont = new Font("Sans-Serif", Font.BOLD, 16);
        this.titleColour = Color.BLACK;

        // Default axis settings.
        this.xAxisLabel = null;
        this.yAxisLabel = null;
        this.axisThickness = 2;
        this.axisColour = Color.BLACK;
        this.axisLabelsFont = new Font("Sans-Serif", Font.PLAIN, 12);
        this.axisLabelColour = Color.BLACK;
        this.axisValuesColour = Color.BLACK;
        this.axisValuesFont = new Font("Sans-Serif", Font.PLAIN, 10);
        this.xAxisValuesFrequency = 1;
        this.xAxisValuesHeight = 0;
        this.xValuesHorizontal = false;
        this.showXAxisValues = true;
        this.showYAxisValues = true;
        this.yAxisValuesFrequency = 1;
        this.yAxisValuesHeight = 0;
        this.yValuesHorizontal = true;

        // Default heatmap settings.
        this.highValueColour = Color.BLACK;
        this.lowValueColour = Color.WHITE;
        this.colourScale = SCALE_LINEAR;

        updateColourDistance();
    }

    /**
     * Returns the low value. This is the value at which the low value colour
     * will be applied.
     *
     * @return the low value.
     */
    public double getLowValue() {
        return lowValue;
    }

    /**
     * Returns the high value. This is the value at which the high value colour
     * will be applied.
     *
     * @return the high value.
     */
    public double getHighValue() {
        return highValue;
    }

    /**
     * Returns the 2-dimensional array of z-values currently in use. Each
     * element is a double array which represents one row of the heat map, or
     * all the z-values for one y-value.
     *
     * @return an array of the z-values in current use, that is, those values
     * which will define the colour of each cell in the resultant heat map.
     */
    public double[][] getZValues() {
        return zValues;
    }

    /**
     * Replaces the z-values array. See the
     * {@link #setZValues(double[][], double, double)} method for an example of
     * z-values. The smallest and largest values in the array are used as the
     * minimum and maximum values respectively.
     * @param zValues the array to replace the current array with. The number
     * of elements in each inner array must be identical.
     */
    public void setZValues(double[][] zValues) {
        setZValues(zValues, min(zValues), max(zValues));
    }

    /**
     * Replaces the z-values array. The number of elements should match the
     * number of y-values, with each element containing a double array with
     * an equal number of elements that matches the number of x-values. Use this
     * method where the minimum and maximum values possible are not contained
     * within the dataset.
     *
     * <h2>Example</h2>
     *
     * <blockcode><pre>
     * new double[][]{
     *   {1.0,1.2,1.4},
     *   {1.2,1.3,1.5},
     *   {0.9,1.3,1.2},
     *   {0.8,1.6,1.1}
     * };
     * </pre></blockcode>
     *
     * The above zValues array is equivalent to:
     *
     * <table border="1">
     *   <tr>
     *     <td rowspan="4" width="20"><center><strong>y</strong></center></td>
     *     <td>1.0</td>
     *     <td>1.2</td>
     *     <td>1.4</td>
     *   </tr>
     *   <tr>
     *     <td>1.2</td>
     *     <td>1.3</td>
     *     <td>1.5</td>
     *   </tr>
     *   <tr>
     *     <td>0.9</td>
     *     <td>1.3</td>
     *     <td>1.2</td>
     *   </tr>
     *   <tr>
     *     <td>0.8</td>
     *     <td>1.6</td>
     *     <td>1.1</td>
     *   </tr>
     *   <tr>
     *     <td></td>
     *     <td colspan="3"><center><strong>x</strong></center></td>
     *   </tr>
     * </table>
     *
     * @param zValues the array to replace the current array with. The number
     * of elements in each inner array must be identical.
     * @param low the minimum possible value, which may or may not appear in the
     * z-values.
     * @param high the maximum possible value, which may or may not appear in
     * the z-values.
     */
    public void setZValues(double[][] zValues, double low, double high) {
        this.zValues = zValues;
        this.lowValue = low;
        this.highValue = high;
    }

    /**
     * Sets the x-values which are plotted along the x-axis. The x-values are
     * calculated based upon the indexes of the z-values array:
     *
     * <blockcode><pre>
     * x-value = x-offset + (column-index * x-interval)
     * </pre></blockcode>
     *
     * <p>The x-interval defines the gap between each x-value and the x-offset
     * is applied to each value to offset them all from zero.
     *
     * <p>Alternatively the x-values can be set more directly with the
     * <code>setXValues(Object[])</code> method.
     *
     * @param xOffset an offset value to be applied to the index of each z-value
     * element.
     * @param xInterval an interval that will separate each x-value item.
     */
    public void setXValues(double xOffset, double xInterval) {
        // Update the x-values according to the offset and interval.
        xValues = new Object[zValues[0].length];
        for (int i=0; i<zValues[0].length; i++) {
            xValues[i] = xOffset + (i * xInterval);
        }
    }

    /**
     * Sets the x-values which are plotted along the x-axis. The given x-values
     * array must be the same length as the z-values array has columns. Each
     * of the x-values elements will be displayed according to their toString
     * representation.
     *
     * @param xValues an array of elements to be displayed as values along the
     * x-axis.
     */
    public void setXValues(Object[] xValues) {
        this.xValues = xValues;
    }

    /**
     * Sets the y-values which are plotted along the y-axis. The y-values are
     * calculated based upon the indexes of the z-values array:
     *
     * <blockcode><pre>
     * y-value = y-offset + (column-index * y-interval)
     * </pre></blockcode>
     *
     * <p>The y-interval defines the gap between each y-value and the y-offset
     * is applied to each value to offset them all from zero.
     *
     * <p>Alternatively the y-values can be set more directly with the
     * <code>setYValues(Object[])</code> method.
     *
     * @param yOffset an offset value to be applied to the index of each z-value
     * element.
     * @param yInterval an interval that will separate each y-value item.
     */
    public void setYValues(double yOffset, double yInterval) {
        // Update the y-values according to the offset and interval.
        yValues = new Object[zValues.length];
        for (int i=0; i<zValues.length; i++) {
            yValues[i] = yOffset + (i * yInterval);
        }
    }

    /**
     * Sets the y-values which are plotted along the y-axis. The given y-values
     * array must be the same length as the z-values array has columns. Each
     * of the y-values elements will be displayed according to their toString
     * representation.
     *
     * @param yValues an array of elements to be displayed as values along the
     * y-axis.
     */
    public void setYValues(Object[] yValues) {
        this.yValues = yValues;
    }

    /**
     * Returns the x-values which are currently set to display along the x-axis.
     * The array that is returned is either that which was explicitly set with
     * <code>setXValues(Object[])</code> or that was generated from the offset
     * and interval that were given to <code>setXValues(double, double)</code>,
     * in which case the object type of each element will be <code>Double</code>.
     *
     * @return an array of the values that are to be displayed along the x-axis.
     */
    public Object[] getXValues() {
        return xValues;
    }

    /**
     * Returns the y-values which are currently set to display along the y-axis.
     * The array that is returned is either that which was explicitly set with
     * <code>setYValues(Object[])</code> or that was generated from the offset
     * and interval that were given to <code>setYValues(double, double)</code>,
     * in which case the object type of each element will be <code>Double</code>.
     *
     * @return an array of the values that are to be displayed along the y-axis.
     */
    public Object[] getYValues() {
        return yValues;
    }

    /**
     * Sets whether the text of the values along the x-axis should be drawn
     * horizontally left-to-right, or vertically top-to-bottom.
     *
     * @param xValuesHorizontal true if x-values should be drawn horizontally,
     * false if they should be drawn vertically.
     */
    public void setXValuesHorizontal(boolean xValuesHorizontal) {
        this.xValuesHorizontal = xValuesHorizontal;
    }

    /**
     * Returns whether the text of the values along the x-axis are to be drawn
     * horizontally left-to-right, or vertically top-to-bottom.
     *
     * @return true if the x-values will be drawn horizontally, false if they
     * will be drawn vertically.
     */
    public boolean isXValuesHorizontal() {
        return xValuesHorizontal;
    }

    /**
     * Sets whether the text of the values along the y-axis should be drawn
     * horizontally left-to-right, or vertically top-to-bottom.
     *
     * @param yValuesHorizontal true if y-values should be drawn horizontally,
     * false if they should be drawn vertically.
     */
    public void setYValuesHorizontal(boolean yValuesHorizontal) {
        this.yValuesHorizontal = yValuesHorizontal;
    }

    /**
     * Returns whether the text of the values along the y-axis are to be drawn
     * horizontally left-to-right, or vertically top-to-bottom.
     *
     * @return true if the y-values will be drawn horizontally, false if they
     * will be drawn vertically.
     */
    public boolean isYValuesHorizontal() {
        return yValuesHorizontal;
    }

    /**
     * Sets the width of each individual cell that constitutes a value in x,y,z
     * data space. By setting the cell width, any previously set chart width
     * will be overwritten with a value calculated based upon this value and the
     * number of cells in there are along the x-axis.
     *
     * @param cellWidth the new width to use for each individual data cell.
     * @deprecated As of release 0.6, replaced by {@link #setCellSize(Dimension)}
     */
    @Deprecated
    public void setCellWidth(int cellWidth) {
        setCellSize(new Dimension(cellWidth, cellSize.height));
    }

    /**
     * Returns the width of each individual data cell that constitutes a value
     * in the x,y,z space.
     *
     * @return the width of each cell.
     * @deprecated As of release 0.6, replaced by {@link #getCellSize}
     */
    @Deprecated
    public int getCellWidth() {
        return cellSize.width;
    }

    /**
     * Sets the height of each individual cell that constitutes a value in x,y,z
     * data space. By setting the cell height, any previously set chart height
     * will be overwritten with a value calculated based upon this value and the
     * number of cells in there are along the y-axis.
     *
     * @param cellHeight the new height to use for each individual data cell.
     * @deprecated As of release 0.6, replaced by {@link #setCellSize(Dimension)}
     */
    @Deprecated
    public void setCellHeight(int cellHeight) {
        setCellSize(new Dimension(cellSize.width, cellHeight));
    }

    /**
     * Returns the height of each individual data cell that constitutes a value
     * in the x,y,z space.
     *
     * @return the height of each cell.
     * @deprecated As of release 0.6, replaced by {@link #getCellSize()}
     */
    @Deprecated
    public int getCellHeight() {
        return cellSize.height;
    }

    /**
     * Sets the size of each individual cell that constitutes a value in x,y,z
     * data space. By setting the cell size, any previously set chart size will
     * be overwritten with a value calculated based upon this value and the
     * number of cells along each axis.
     *
     * @param cellSize the new size to use for each individual data cell.
     * @since 0.6
     */
    public void setCellSize(Dimension cellSize) {
        this.cellSize = cellSize;
    }

    /**
     * Returns the size of each individual data cell that constitutes a value in
     * the x,y,z space.
     *
     * @return the size of each individual data cell.
     * @since 0.6
     */
    public Dimension getCellSize() {
        return cellSize;
    }

    /**
     * Returns the width of the chart in pixels as calculated according to the
     * cell dimensions, chart margin and other size settings.
     *
     * @return the width in pixels of the chart image to be generated.
     * @deprecated As of release 0.6, replaced by {@link #getChartSize()}
     */
    @Deprecated
    public int getChartWidth() {
        return chartSize.width;
    }

    /**
     * Returns the height of the chart in pixels as calculated according to the
     * cell dimensions, chart margin and other size settings.
     *
     * @return the height in pixels of the chart image to be generated.
     * @deprecated As of release 0.6, replaced by {@link #getChartSize()}
     */
    @Deprecated
    public int getChartHeight() {
        return chartSize.height;
    }

    /**
     * Returns the size of the chart in pixels as calculated according to the
     * cell dimensions, chart margin and other size settings.
     *
     * @return the size in pixels of the chart image to be generated.
     * @since 0.6
     */
    public Dimension getChartSize() {
        return chartSize;
    }

    /**
     * Returns the String that will be used as the title of any successive
     * calls to generate a chart.
     *
     * @return the title of the chart.
     */
    public String getTitle() {
        return title;
    }

    /**
     * Sets the String that will be used as the title of any successive
     * calls to generate a chart. The title will be displayed centralised
     * horizontally at the top of any generated charts.
     *
     * <p>
     * If the title is set to <tt>null</tt> then no title will be displayed.
     *
     * <p>
     * Defaults to null.
     *
     * @param title the chart title to set.
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Returns the String that will be displayed as a description of the
     * x-axis in any generated charts.
     *
     * @return the display label describing the x-axis.
     */
    public String getXAxisLabel() {
        return xAxisLabel;
    }

    /**
     * Sets the String that will be displayed as a description of the
     * x-axis in any generated charts. The label will be displayed
     * horizontally central of the x-axis bar.
     *
     * <p>
     * If the xAxisLabel is set to <tt>null</tt> then no label will be
     * displayed.
     *
     * <p>
     * Defaults to null.
     *
     * @param xAxisLabel the label to be displayed describing the x-axis.
     */
    public void setXAxisLabel(String xAxisLabel) {
        this.xAxisLabel = xAxisLabel;
    }

    /**
     * Returns the String that will be displayed as a description of the
     * y-axis in any generated charts.
     *
     * @return the display label describing the y-axis.
     */
    public String getYAxisLabel() {
        return yAxisLabel;
    }

    /**
     * Sets the String that will be displayed as a description of the
     * y-axis in any generated charts. The label will be displayed
     * horizontally central of the y-axis bar.
     *
     * <p>
     * If the yAxisLabel is set to <tt>null</tt> then no label will be
     * displayed.
     *
     * <p>
     * Defaults to null.
     *
     * @param yAxisLabel the label to be displayed describing the y-axis.
     */
    public void setYAxisLabel(String yAxisLabel) {
        this.yAxisLabel = yAxisLabel;
    }

    /**
     * Returns the width of the margin in pixels to be left as empty space
     * around the heat map element.
     *
     * @return the size of the margin to be left blank around the edge of the
     * chart.
     */
    public int getChartMargin() {
        return margin;
    }

    /**
     * Sets the width of the margin in pixels to be left as empty space around
     * the heat map element. If a title is set then half the margin will be
     * directly above the title and half directly below it. Where axis labels
     * are set then the axis labels may sit partially in the margin.
     *
     * <p>
     * Defaults to 20 pixels.
     *
     * @param margin the new margin to be left as blank space around the heat
     * map.
     */
    public void setChartMargin(int margin) {
        this.margin = margin;
    }

    /**
     * Returns an object that represents the colour to be used as the
     * background for the whole chart.
     *
     * @return the colour to be used to fill the chart background.
     */
    public Color getBackgroundColour() {
        return backgroundColour;
    }

    /**
     * Sets the colour to be used on the background of the chart. A transparent
     * background can be set by setting a background colour with an alpha value.
     * The transparency will only be effective when the image is saved as a png
     * or gif.
     *
     * <p>
     * Defaults to <code>Color.WHITE</code>.
     *
     * @param backgroundColour the new colour to be set as the background fill.
     */
    public void setBackgroundColour(Color backgroundColour) {
        if (backgroundColour == null) {
            backgroundColour = Color.WHITE;
        }

        this.backgroundColour = backgroundColour;
    }

    /**
     * Returns the <code>Font</code> that describes the visual style of the
     * title.
     *
     * @return the Font that will be used to render the title.
     */
    public Font getTitleFont() {
        return titleFont;
    }

    /**
     * Sets a new <code>Font</code> to be used in rendering the chart's title
     * String.
     *
     * <p>
     * Defaults to Sans-Serif, BOLD, 16 pixels.
     *
     * @param titleFont the Font that should be used when rendering the chart
     * title.
     */
    public void setTitleFont(Font titleFont) {
        this.titleFont = titleFont;
    }

    /**
     * Returns the <code>Color</code> that represents the colour the title text
     * should be painted in.
     *
     * @return the currently set colour to be used in painting the chart title.
     */
    public Color getTitleColour() {
        return titleColour;
    }

    /**
     * Sets the <code>Color</code> that describes the colour to be used for the
     * chart title String.
     *
     * <p>
     * Defaults to <code>Color.BLACK</code>.
     *
     * @param titleColour the colour to paint the chart's title String.
     */
    public void setTitleColour(Color titleColour) {
        this.titleColour = titleColour;
    }

    /**
     * Returns the width of the axis bars in pixels. Both axis bars have the
     * same thickness.
     *
     * @return the thickness of the axis bars in pixels.
     */
    public int getAxisThickness() {
        return axisThickness;
    }

    /**
     * Sets the width of the axis bars in pixels. Both axis bars use the same
     * thickness.
     *
     * <p>
     * Defaults to 2 pixels.
     *
     * @param axisThickness the thickness to use for the axis bars in any newly
     * generated charts.
     */
    public void setAxisThickness(int axisThickness) {
        this.axisThickness = axisThickness;
    }

    /**
     * Returns the colour that is set to be used for the axis bars. Both axis
     * bars use the same colour.
     *
     * @return the colour in use for the axis bars.
     */
    public Color getAxisColour() {
        return axisColour;
    }

    /**
     * Sets the colour to be used on the axis bars. Both axis bars use the same
     * colour.
     *
     * <p>
     * Defaults to <code>Color.BLACK</code>.
     *
     * @param axisColour the colour to be set for use on the axis bars.
     */
    public void setAxisColour(Color axisColour) {
        this.axisColour = axisColour;
    }

    /**
     * Returns the font that describes the visual style of the labels of the
     * axis. Both axis' labels use the same font.
     *
     * @return the font used to define the visual style of the axis labels.
     */
    public Font getAxisLabelsFont() {
        return axisLabelsFont;
    }

    /**
     * Sets the font that describes the visual style of the axis labels. Both
     * axis' labels use the same font.
     *
     * <p>
     * Defaults to Sans-Serif, PLAIN, 12 pixels.
     *
     * @param axisLabelsFont the font to be used to define the visual style of
     * the axis labels.
     */
    public void setAxisLabelsFont(Font axisLabelsFont) {
        this.axisLabelsFont = axisLabelsFont;
    }

    /**
     * Returns the current colour of the axis labels. Both labels use the same
     * colour.
     *
     * @return the colour of the axis label text.
     */
    public Color getAxisLabelColour() {
        return axisLabelColour;
    }

    /**
     * Sets the colour of the text displayed as axis labels. Both labels use
     * the same colour.
     *
     * <p>
     * Defaults to Color.BLACK.
     *
     * @param axisLabelColour the colour to use for the axis label text.
     */
    public void setAxisLabelColour(Color axisLabelColour) {
        this.axisLabelColour = axisLabelColour;
    }

    /**
     * Returns the font which describes the visual style of the axis values.
     * The axis values are those values displayed alongside the axis bars at
     * regular intervals. Both axis use the same font.
     *
     * @return the font in use for the axis values.
     */
    public Font getAxisValuesFont() {
        return axisValuesFont;
    }

    /**
     * Sets the font which describes the visual style of the axis values. The
     * axis values are those values displayed alongside the axis bars at
     * regular intervals. Both axis use the same font.
     *
     * <p>
     * Defaults to Sans-Serif, PLAIN, 10 pixels.
     *
     * @param axisValuesFont the font that should be used for the axis values.
     */
    public void setAxisValuesFont(Font axisValuesFont) {
        this.axisValuesFont = axisValuesFont;
    }

    /**
     * Returns the colour of the axis values as they will be painted along the
     * axis bars. Both axis use the same colour.
     *
     * @return the colour of the values displayed along the axis bars.
     */
    public Color getAxisValuesColour() {
        return axisValuesColour;
    }

    /**
     * Sets the colour to be used for the axis values as they will be painted
     * along the axis bars. Both axis use the same colour.
     *
     * <p>
     * Defaults to Color.BLACK.
     *
     * @param axisValuesColour the new colour to be used for the axis bar values.
     */
    public void setAxisValuesColour(Color axisValuesColour) {
        this.axisValuesColour = axisValuesColour;
    }

    /**
     * Returns the frequency of the values displayed along the x-axis. The
     * frequency is how many columns in the x-dimension have their value
     * displayed. A frequency of 2 would mean every other column has a value
     * shown and a frequency of 3 would mean every third column would be given a
     * value.
     *
     * @return the frequency of the values displayed against columns.
     */
    public int getXAxisValuesFrequency() {
        return xAxisValuesFrequency;
    }

    /**
     * Sets the frequency of the values displayed along the x-axis. The
     * frequency is how many columns in the x-dimension have their value
     * displayed. A frequency of 2 would mean every other column has a value and
     * a frequency of 3 would mean every third column would be given a value.
     *
     * <p>
     * Defaults to 1. Every column is given a value.
     *
     * @param axisValuesFrequency the frequency of the values displayed against
     * columns, where 1 is every column and 2 is every other column.
     */
    public void setXAxisValuesFrequency(int axisValuesFrequency) {
        this.xAxisValuesFrequency = axisValuesFrequency;
    }

    /**
     * Returns the frequency of the values displayed along the y-axis. The
     * frequency is how many rows in the y-dimension have their value displayed.
     * A frequency of 2 would mean every other row has a value and a frequency
     * of 3 would mean every third row would be given a value.
     *
     * @return the frequency of the values displayed against rows.
     */
    public int getYAxisValuesFrequency() {
        return yAxisValuesFrequency;
    }

    /**
     * Sets the frequency of the values displayed along the y-axis. The
     * frequency is how many rows in the y-dimension have their value displayed.
     * A frequency of 2 would mean every other row has a value and a frequency
     * of 3 would mean every third row would be given a value.
     *
     * <p>
     * Defaults to 1. Every row is given a value.
     *
     * @param axisValuesFrequency the frequency of the values displayed against
     * rows, where 1 is every row and 2 is every other row.
     */
    public void setYAxisValuesFrequency(int axisValuesFrequency) {
        yAxisValuesFrequency = axisValuesFrequency;
    }

    /**
     * Returns whether axis values are to be shown at all for the x-axis.
     *
     * <p>
     * If axis values are not shown then more space is allocated to the heat
     * map.
     *
     * @return true if the x-axis values will be displayed, false otherwise.
     */
    public boolean isShowXAxisValues() {
        //TODO Could get rid of these flags and use a frequency of -1 to signal no values.
        return showXAxisValues;
    }

    /**
     * Sets whether axis values are to be shown at all for the x-axis.
     *
     * <p>
     * If axis values are not shown then more space is allocated to the heat
     * map.
     *
     * <p>
     * Defaults to true.
     *
     * @param showXAxisValues true if x-axis values should be displayed, false
     * if they should be hidden.
     */
    public void setShowXAxisValues(boolean showXAxisValues) {
        this.showXAxisValues = showXAxisValues;
    }

    /**
     * Returns whether axis values are to be shown at all for the y-axis.
     *
     * <p>
     * If axis values are not shown then more space is allocated to the heat
     * map.
     *
     * @return true if the y-axis values will be displayed, false otherwise.
     */
    public boolean isShowYAxisValues() {
        return showYAxisValues;
    }

    /**
     * Sets whether axis values are to be shown at all for the y-axis.
     *
     * <p>
     * If axis values are not shown then more space is allocated to the heat
     * map.
     *
     * <p>
     * Defaults to true.
     *
     * @param showYAxisValues true if y-axis values should be displayed, false
     * if they should be hidden.
     */
    public void setShowYAxisValues(boolean showYAxisValues) {
        this.showYAxisValues = showYAxisValues;
    }

    /**
     * Returns the colour that is currently to be displayed for the heat map
     * cells with the highest z-value in the dataset.
     *
     * <p>
     * The full colour range will go through each RGB step between the high
     * value colour and the low value colour.
     *
     * @return the colour in use for cells of the highest z-value.
     */
    public Color getHighValueColour() {
        return highValueColour;
    }

    /**
     * Sets the colour to be used to fill cells of the heat map with the
     * highest z-values in the dataset.
     *
     * <p>
     * The full colour range will go through each RGB step between the high
     * value colour and the low value colour.
     *
     * <p>
     * Defaults to Color.BLACK.
     *
     * @param highValueColour the colour to use for cells of the highest
     * z-value.
     */
    public void setHighValueColour(Color highValueColour) {
        this.highValueColour = highValueColour;

        updateColourDistance();
    }

    /**
     * Returns the colour that is currently to be displayed for the heat map
     * cells with the lowest z-value in the dataset.
     *
     * <p>
     * The full colour range will go through each RGB step between the high
     * value colour and the low value colour.
     *
     * @return the colour in use for cells of the lowest z-value.
     */
    public Color getLowValueColour() {
        return lowValueColour;
    }

    /**
     * Sets the colour to be used to fill cells of the heat map with the
     * lowest z-values in the dataset.
     *
     * <p>
     * The full colour range will go through each RGB step between the high
     * value colour and the low value colour.
     *
     * <p>
     * Defaults to Color.WHITE.
     *
     * @param lowValueColour the colour to use for cells of the lowest
     * z-value.
     */
    public void setLowValueColour(Color lowValueColour) {
        this.lowValueColour = lowValueColour;

        updateColourDistance();
    }

    /**
     * Returns the scale that is currently in use to map z-value to colour. A
     * value of 1.0 will give a <strong>linear</strong> scale, which will
     * spread the distribution of colours evenly amoungst the full range of
     * represented z-values. A value of greater than 1.0 will give an
     * <strong>exponential</strong> scale that will produce greater emphasis
     * for the separation between higher values and a value between 0.0 and 1.0
     * will provide a <strong>logarithmic</strong> scale, with greater
     * separation of low values.
     *
     * @return the scale factor that is being used to map from z-value to colour.
     */
    public double getColourScale() {
        return colourScale;
    }

    /**
     * Sets the scale that is currently in use to map z-value to colour. A
     * value of 1.0 will give a <strong>linear</strong> scale, which will
     * spread the distribution of colours evenly amoungst the full range of
     * represented z-values. A value of greater than 1.0 will give an
     * <strong>exponential</strong> scale that will produce greater emphasis
     * for the separation between higher values and a value between 0.0 and 1.0
     * will provide a <strong>logarithmic</strong> scale, with greater
     * separation of low values. Values of 0.0 or less are illegal.
     *
     * <p>
     * Defaults to a linear scale value of 1.0.
     *
     * @param colourScale the scale that should be used to map from z-value to
     * colour.
     */
    public void setColourScale(double colourScale) {
        this.colourScale = colourScale;
    }

    /*
     * Calculate and update the field for the distance between the low colour
     * and high colour. The distance is the number of steps between one colour
     * and the other using an RGB coding with 0-255 values for each of red,
     * green and blue. So the maximum colour distance is 255 + 255 + 255.
     */
    private void updateColourDistance() {
        int r1 = lowValueColour.getRed();
        int g1 = lowValueColour.getGreen();
        int b1 = lowValueColour.getBlue();
        int r2 = highValueColour.getRed();
        int g2 = highValueColour.getGreen();
        int b2 = highValueColour.getBlue();

        colourValueDistance = Math.abs(r1 - r2);
        colourValueDistance += Math.abs(g1 - g2);
        colourValueDistance += Math.abs(b1 - b2);
    }

    /**
     * Generates a new chart <code>Image</code> based upon the currently held
     * settings and then attempts to save that image to disk, to the location
     * provided as a File parameter. The image type of the saved file will
     * equal the extension of the filename provided, so it is essential that a
     * suitable extension be included on the file name.
     *
     * <p>
     * All supported <code>ImageIO</code> file types are supported, including
     * PNG, JPG and GIF.
     *
     * <p>
     * No chart will be generated until this or the related
     * <code>getChartImage()</code> method are called. All successive calls
     * will result in the generation of a new chart image, no caching is used.
     *
     * @param outputFile the file location that the generated image file should
     * be written to. The File must have a suitable filename, with an extension
     * of a valid image format (as supported by <code>ImageIO</code>).
     * @throws IOException if the output file's filename has no extension or
     * if there the file is unable to written to. Reasons for this include a
     * non-existant file location (check with the File exists() method on the
     * parent directory), or the permissions of the write location may be
     * incorrect.
     */
    public void saveToFile(File outputFile) throws IOException {
        String filename = outputFile.getName();

        int extPoint = filename.lastIndexOf('.');

        if (extPoint < 0) {
            throw new IOException("Illegal filename, no extension used.");
        }

        // Determine the extension of the filename.
        String ext = filename.substring(extPoint + 1);

        // Handle jpg without transparency.
        if (ext.toLowerCase().equals("jpg") || ext.toLowerCase().equals("jpeg")) {
            BufferedImage chart = (BufferedImage) getChartImage(false);

            // Save our graphic.
            saveGraphicJpeg(chart, outputFile, 1.0f);
        } else {
            BufferedImage chart = (BufferedImage) getChartImage(true);

            ImageIO.write(chart, ext, outputFile);
        }
    }

    private void saveGraphicJpeg(BufferedImage chart, File outputFile, float quality) throws IOException {
        // Setup correct compression for jpeg.
        Iterator<ImageWriter> iter = ImageIO.getImageWritersByFormatName("jpeg");
        ImageWriter writer = (ImageWriter) iter.next();
        ImageWriteParam iwp = writer.getDefaultWriteParam();
        iwp.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        iwp.setCompressionQuality(quality);

        // Output the image.
        FileImageOutputStream output = new FileImageOutputStream(outputFile);
        writer.setOutput(output);
        IIOImage image = new IIOImage(chart, null, null);
        writer.write(null, image, iwp);
        writer.dispose();

    }

    /**
     * Generates and returns a new chart <code>Image</code> configured
     * according to this object's currently held settings. The given parameter
     * determines whether transparency should be enabled for the generated
     * image.
     *
     * <p>
     * No chart will be generated until this or the related
     * <code>saveToFile(File)</code> method are called. All successive calls
     * will result in the generation of a new chart image, no caching is used.
     *
     * @param alpha whether to enable transparency.
     * @return A newly generated chart <code>Image</code>. The returned image
     * is a <code>BufferedImage</code>.
     */
    public Image getChartImage(boolean alpha) {
        // Calculate all unknown dimensions.
        measureComponents();
        updateCoordinates();

        // Determine image type based upon whether require alpha or not.
        // Using BufferedImage.TYPE_INT_ARGB seems to break on jpg.
        int imageType = (alpha ? BufferedImage.TYPE_4BYTE_ABGR : BufferedImage.TYPE_3BYTE_BGR);

        // Create our chart image which we will eventually draw everything on.
        BufferedImage chartImage = new BufferedImage(chartSize.width, chartSize.height, imageType);
        Graphics2D chartGraphics = chartImage.createGraphics();

        // Use anti-aliasing where ever possible.
        chartGraphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
            RenderingHints.VALUE_ANTIALIAS_ON);

        // Set the background.
        chartGraphics.setColor(backgroundColour);
        chartGraphics.fillRect(0, 0, chartSize.width, chartSize.height);

        // Draw the title.
        drawTitle(chartGraphics);

        // Draw the heatmap image.
        drawHeatMap(chartGraphics, zValues);

        // Draw the axis labels.
        drawXLabel(chartGraphics);
        drawYLabel(chartGraphics);

        // Draw the axis bars.
        drawAxisBars(chartGraphics);

        // Draw axis values.
        drawXValues(chartGraphics);
        drawYValues(chartGraphics);

        return chartImage;
    }

    /**
     * Generates and returns a new chart <code>Image</code> configured
     * according to this object's currently held settings. By default the image
     * is generated with no transparency.
     *
     * <p>
     * No chart will be generated until this or the related
     * <code>saveToFile(File)</code> method are called. All successive calls
     * will result in the generation of a new chart image, no caching is used.
     *
     * @return A newly generated chart <code>Image</code>. The returned image
     * is a <code>BufferedImage</code>.
     */
    public Image getChartImage() {
        return getChartImage(false);
    }

    /*
     * Calculates all unknown component dimensions.
     */
    private void measureComponents() {
        //TODO This would be a good place to check that all settings have sensible values or throw illegal state exception.

        //TODO Put this somewhere so it only gets created once.
        BufferedImage chartImage = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB);
        Graphics2D tempGraphics = chartImage.createGraphics();

        // Calculate title dimensions.
        if (title != null) {
            tempGraphics.setFont(titleFont);
            FontMetrics metrics = tempGraphics.getFontMetrics();
            titleSize = new Dimension(metrics.stringWidth(title), metrics.getHeight());
            titleAscent = metrics.getAscent();
        } else {
            titleSize = new Dimension(0, 0);
        }

        // Calculate x-axis label dimensions.
        if (xAxisLabel != null) {
            tempGraphics.setFont(axisLabelsFont);
            FontMetrics metrics = tempGraphics.getFontMetrics();
            xAxisLabelSize = new Dimension(metrics.stringWidth(xAxisLabel), metrics.getHeight());
            xAxisLabelDescent = metrics.getDescent();
        } else {
            xAxisLabelSize = new Dimension(0, 0);
        }

        // Calculate y-axis label dimensions.
        if (yAxisLabel != null) {
            tempGraphics.setFont(axisLabelsFont);
            FontMetrics metrics = tempGraphics.getFontMetrics();
            yAxisLabelSize = new Dimension(metrics.stringWidth(yAxisLabel), metrics.getHeight());
            yAxisLabelAscent = metrics.getAscent();
        } else {
            yAxisLabelSize = new Dimension(0, 0);
        }

        // Calculate x-axis value dimensions.
        if (showXAxisValues) {
            tempGraphics.setFont(axisValuesFont);
            FontMetrics metrics = tempGraphics.getFontMetrics();
            xAxisValuesHeight = metrics.getHeight();
            xAxisValuesWidthMax = 0;
            for (Object o: xValues) {
                int w = metrics.stringWidth(o.toString());
                if (w > xAxisValuesWidthMax) {
                    xAxisValuesWidthMax = w;
                }
            }
        } else {
            xAxisValuesHeight = 0;
        }

        // Calculate y-axis value dimensions.
        if (showYAxisValues) {
            tempGraphics.setFont(axisValuesFont);
            FontMetrics metrics = tempGraphics.getFontMetrics();
            yAxisValuesHeight = metrics.getHeight();
            yAxisValuesAscent = metrics.getAscent();
            yAxisValuesWidthMax = 0;
            for (Object o: yValues) {
                int w = metrics.stringWidth(o.toString());
                if (w > yAxisValuesWidthMax) {
                    yAxisValuesWidthMax = w;
                }
            }
        } else {
            yAxisValuesHeight = 0;
        }

        // Calculate heatmap dimensions.
        int heatMapWidth = (zValues[0].length * cellSize.width);
        int heatMapHeight = (zValues.length * cellSize.height);
        heatMapSize = new Dimension(heatMapWidth, heatMapHeight);

        int yValuesHorizontalSize = 0;
        if (yValuesHorizontal) {
            yValuesHorizontalSize = yAxisValuesWidthMax;
        } else {
            yValuesHorizontalSize = yAxisValuesHeight;
        }

        int xValuesVerticalSize = 0;
        if (xValuesHorizontal) {
            xValuesVerticalSize = xAxisValuesHeight;
        } else {
            xValuesVerticalSize = xAxisValuesWidthMax;
        }

        // Calculate chart dimensions.
        int chartWidth = heatMapWidth + (2 * margin) + yAxisLabelSize.height + yValuesHorizontalSize + axisThickness;
        int chartHeight = heatMapHeight + (2 * margin) + xAxisLabelSize.height + xValuesVerticalSize + titleSize.height + axisThickness;
        chartSize = new Dimension(chartWidth, chartHeight);
    }

    /*
     * Calculates the co-ordinates of some key positions.
     */
    private void updateCoordinates() {
        // Top-left of heat map.
        int x = margin + axisThickness + yAxisLabelSize.height;
        x += (yValuesHorizontal ? yAxisValuesWidthMax : yAxisValuesHeight);
        int y = titleSize.height + margin;
        heatMapTL = new Point(x, y);

        // Top-right of heat map.
        x = heatMapTL.x + heatMapSize.width;
        y = heatMapTL.y + heatMapSize.height;
        heatMapBR = new Point(x, y);

        // Centre of heat map.
        x = heatMapTL.x + (heatMapSize.width / 2);
        y = heatMapTL.y + (heatMapSize.height / 2);
        heatMapC = new Point(x, y);
    }

    /*
     * Draws the title String on the chart if title is not null.
     */
    private void drawTitle(Graphics2D chartGraphics) {
        if (title != null) {
            // Strings are drawn from the baseline position of the leftmost char.
            int yTitle = (margin/2) + titleAscent;
            int xTitle = (chartSize.width/2) - (titleSize.width/2);

            chartGraphics.setFont(titleFont);
            chartGraphics.setColor(titleColour);
            chartGraphics.drawString(title, xTitle, yTitle);
        }
    }

    /*
     * Creates the actual heatmap element as an image, that can then be drawn
     * onto a chart.
     */
    private void drawHeatMap(Graphics2D chartGraphics, double[][] data) {
        // Calculate the available size for the heatmap.
        int noYCells = data.length;
        int noXCells = data[0].length;

        //double dataMin = min(data);
        //double dataMax = max(data);

        BufferedImage heatMapImage = new BufferedImage(heatMapSize.width, heatMapSize.height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D heatMapGraphics = heatMapImage.createGraphics();

        for (int x=0; x<noXCells; x++) {
            for (int y=0; y<noYCells; y++) {
                // Set colour depending on zValues.
                heatMapGraphics.setColor(getCellColour(data[y][x], lowValue, highValue));

                int cellX = x*cellSize.width;
                int cellY = y*cellSize.height;

                heatMapGraphics.fillRect(cellX, cellY, cellSize.width, cellSize.height);
            }
        }

        // Draw the heat map onto the chart.
        chartGraphics.drawImage(heatMapImage, heatMapTL.x, heatMapTL.y, heatMapSize.width, heatMapSize.height, null);
    }

    /*
     * Draws the x-axis label string if it is not null.
     */
    private void drawXLabel(Graphics2D chartGraphics) {
        if (xAxisLabel != null) {
            // Strings are drawn from the baseline position of the leftmost char.
            int yPosXAxisLabel = chartSize.height - (margin / 2) - xAxisLabelDescent;
            //TODO This will need to be updated if the y-axis values/label can be moved to the right.
            int xPosXAxisLabel = heatMapC.x - (xAxisLabelSize.width / 2);

            chartGraphics.setFont(axisLabelsFont);
            chartGraphics.setColor(axisLabelColour);
            chartGraphics.drawString(xAxisLabel, xPosXAxisLabel, yPosXAxisLabel);
        }
    }

    /*
     * Draws the y-axis label string if it is not null.
     */
    private void drawYLabel(Graphics2D chartGraphics) {
        if (yAxisLabel != null) {
            // Strings are drawn from the baseline position of the leftmost char.
            int yPosYAxisLabel = heatMapC.y + (yAxisLabelSize.width / 2);
            int xPosYAxisLabel = (margin / 2) + yAxisLabelAscent;

            chartGraphics.setFont(axisLabelsFont);
            chartGraphics.setColor(axisLabelColour);

            // Create 270 degree rotated transform.
            AffineTransform transform = chartGraphics.getTransform();
            AffineTransform originalTransform = (AffineTransform) transform.clone();
            transform.rotate(Math.toRadians(270), xPosYAxisLabel, yPosYAxisLabel);
            chartGraphics.setTransform(transform);

            // Draw string.
            chartGraphics.drawString(yAxisLabel, xPosYAxisLabel, yPosYAxisLabel);

            // Revert to original transform before rotation.
            chartGraphics.setTransform(originalTransform);
        }
    }

    /*
     * Draws the bars of the x-axis and y-axis.
     */
    private void drawAxisBars(Graphics2D chartGraphics) {
        if (axisThickness > 0) {
            chartGraphics.setColor(axisColour);

            // Draw x-axis.
            int x = heatMapTL.x - axisThickness;
            int y = heatMapBR.y;
            int width = heatMapSize.width + axisThickness;
            int height = axisThickness;
            chartGraphics.fillRect(x, y, width, height);

            // Draw y-axis.
            x = heatMapTL.x - axisThickness;
            y = heatMapTL.y;
            width = axisThickness;
            height = heatMapSize.height;
            chartGraphics.fillRect(x, y, width, height);
        }
    }

    /*
     * Draws the x-values onto the x-axis if showXAxisValues is set to true.
     */
    private void drawXValues(Graphics2D chartGraphics) {
        if (!showXAxisValues) {
            return;
        }

        chartGraphics.setColor(axisValuesColour);

        for (int i=0; i<xValues.length; i++) {
            if (i % xAxisValuesFrequency != 0) {
                continue;
            }

            String xValueStr = xValues[i].toString();

            chartGraphics.setFont(axisValuesFont);
            FontMetrics metrics = chartGraphics.getFontMetrics();

            int valueWidth = metrics.stringWidth(xValueStr);

            if (xValuesHorizontal) {
                // Draw the value with whatever font is now set.
                int valueXPos = (i * cellSize.width) + ((cellSize.width / 2) - (valueWidth / 2));
                valueXPos += heatMapTL.x;
                int valueYPos = heatMapBR.y + metrics.getAscent() + 1;

                chartGraphics.drawString(xValueStr, valueXPos, valueYPos);
            } else {
                int valueXPos = heatMapTL.x + (i * cellSize.width) + ((cellSize.width / 2) + (xAxisValuesHeight / 2));
                int valueYPos = heatMapBR.y + axisThickness + valueWidth;

                // Create 270 degree rotated transform.
                AffineTransform transform = chartGraphics.getTransform();
                AffineTransform originalTransform = (AffineTransform) transform.clone();
                transform.rotate(Math.toRadians(270), valueXPos, valueYPos);
                chartGraphics.setTransform(transform);

                // Draw the string.
                chartGraphics.drawString(xValueStr, valueXPos, valueYPos);

                // Revert to original transform before rotation.
                chartGraphics.setTransform(originalTransform);
            }
        }
    }

    /*
     * Draws the y-values onto the y-axis if showYAxisValues is set to true.
     */
    private void drawYValues(Graphics2D chartGraphics) {
        if (!showYAxisValues) {
            return;
        }

        chartGraphics.setColor(axisValuesColour);

        for (int i=0; i<yValues.length; i++) {
            if (i % yAxisValuesFrequency != 0) {
                continue;
            }

            String yValueStr = yValues[i].toString();

            chartGraphics.setFont(axisValuesFont);
            FontMetrics metrics = chartGraphics.getFontMetrics();

            int valueWidth = metrics.stringWidth(yValueStr);

            if (yValuesHorizontal) {
                // Draw the value with whatever font is now set.
                int valueXPos = margin + yAxisLabelSize.height + (yAxisValuesWidthMax - valueWidth);
                int valueYPos = heatMapTL.y + (i * cellSize.height) + (cellSize.height/2) + (yAxisValuesAscent/2);

                chartGraphics.drawString(yValueStr, valueXPos, valueYPos);
            } else {
                int valueXPos = margin + yAxisLabelSize.height + yAxisValuesAscent;
                int valueYPos = heatMapTL.y + (i * cellSize.height) + (cellSize.height/2) + (valueWidth/2);

                // Create 270 degree rotated transform.
                AffineTransform transform = chartGraphics.getTransform();
                AffineTransform originalTransform = (AffineTransform) transform.clone();
                transform.rotate(Math.toRadians(270), valueXPos, valueYPos);
                chartGraphics.setTransform(transform);

                // Draw the string.
                chartGraphics.drawString(yValueStr, valueXPos, valueYPos);

                // Revert to original transform before rotation.
                chartGraphics.setTransform(originalTransform);
            }
        }
    }

    /*
     * Determines what colour a heat map cell should be based upon the cell
     * values.
     */
    private Color getCellColour(double data, double min, double max) {
        double range = max - min;
        double position = data - min;

        // What proportion of the way through the possible values is that.
        double percentPosition = position / range;

        // Which colour group does that put us in.
        int colourPosition = getColourPosition(percentPosition);

        int r = lowValueColour.getRed();
        int g = lowValueColour.getGreen();
        int b = lowValueColour.getBlue();

        // Make n shifts of the colour, where n is the colourPosition.
        for (int i=0; i<colourPosition; i++) {
            int rDistance = r - highValueColour.getRed();
            int gDistance = g - highValueColour.getGreen();
            int bDistance = b - highValueColour.getBlue();

            if ((Math.abs(rDistance) >= Math.abs(gDistance))
                && (Math.abs(rDistance) >= Math.abs(bDistance))) {
                // Red must be the largest.
                r = changeColourValue(r, rDistance);
            } else if (Math.abs(gDistance) >= Math.abs(bDistance)) {
                // Green must be the largest.
                g = changeColourValue(g, gDistance);
            } else {
                // Blue must be the largest.
                b = changeColourValue(b, bDistance);
            }
        }

        return new Color(r, g, b);
    }

    /*
     * Returns how many colour shifts are required from the lowValueColour to
     * get to the correct colour position. The result will be different
     * depending on the colour scale used: LINEAR, LOGARITHMIC, EXPONENTIAL.
     */
    private int getColourPosition(double percentPosition) {
        return (int) Math.round(colourValueDistance * Math.pow(percentPosition, colourScale));
    }

    private int changeColourValue(int colourValue, int colourDistance) {
        if (colourDistance < 0) {
            return colourValue+1;
        } else if (colourDistance > 0) {
            return colourValue-1;
        } else {
            // This shouldn't actually happen here.
            return colourValue;
        }
    }

    /**
     * Finds and returns the maximum value in a 2-dimensional array of doubles.
     *
     * @return the largest value in the array.
     */
    public static double max(double[][] values) {
        double max = 0;
        for (int i=0; i<values.length; i++) {
            for (int j=0; j<values[i].length; j++) {
                max = (values[i][j] > max) ? values[i][j] : max;
            }
        }
        return max;
    }

    /**
     * Finds and returns the minimum value in a 2-dimensional array of doubles.
     *
     * @return the smallest value in the array.
     */
    public static double min(double[][] values) {
        double min = Double.MAX_VALUE;
        for (int i=0; i<values.length; i++) {
            for (int j=0; j<values[i].length; j++) {
                min = (values[i][j] < min) ? values[i][j] : min;
            }
        }
        return min;
    }

}
