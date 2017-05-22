package com.spotify.heroic.http.render;

/**
 * Created by lucile on 22/05/17.
 */

import javax.swing.*;
import java.awt.*;

    /**
     * <p>This class is a very simple example of how to use the HeatMap class.</p>
     *
     * <hr />
     * <p><strong>Copyright:</strong> Copyright (c) 2007, 2008</p>
     *
     * <p>HeatMap is free software; you can redistribute it and/or modify
     * it under the terms of the GNU General Public License as published by
     * the Free Software Foundation; either version 2 of the License, or
     * (at your option) any later version.</p>
     *
     * <p>HeatMap is distributed in the hope that it will be useful,
     * but WITHOUT ANY WARRANTY; without even the implied warranty of
     * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
     * GNU General Public License for more details.</p>
     *
     * <p>You should have received a copy of the GNU General Public License
     * along with HeatMap; if not, write to the Free Software
     * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA</p>
     *
     * @author Matthew Beckler (matthew@mbeckler.org)
     * @author Josh Hayes-Sheen (grey@grevian.org), Converted to use BufferedImage.
     * @author J. Keller (jpaulkeller@gmail.com), Added transparency (alpha) support, data ordering bug fix.
     * @version 1.6
     */

class HeatMapFrame extends JFrame
{
    HeatMap panel;
    public HeatMapFrame() throws Exception
    {
        super("Heat Map Frame");
        double[][] data = HeatMap.generateRampTestData();
        boolean useGraphicsYAxis = true;

        // you can use a pre-defined gradient:
        panel = new HeatMap(data, useGraphicsYAxis, Gradient.GRADIENT_BLUE_TO_RED);

        // or you can also make a custom gradient:
        Color[] gradientColors = new Color[]{Color.blue, Color.green, Color.yellow};
        Color[] customGradient = Gradient.createMultiGradient(gradientColors, 500);
        panel.updateGradient(customGradient);

        // set miscelaneous settings
        panel.setDrawLegend(true);

        panel.setTitle("Height (m)");
        panel.setDrawTitle(true);

        panel.setXAxisTitle("X-Distance (m)");
        panel.setDrawXAxisTitle(true);

        panel.setYAxisTitle("Y-Distance (m)");
        panel.setDrawYAxisTitle(true);

        panel.setCoordinateBounds(0, 6.28, 0, 6.28);
        panel.setDrawXTicks(true);
        panel.setDrawYTicks(true);

        panel.setColorForeground(Color.black);
        panel.setColorBackground(Color.white);

        this.getContentPane().add(panel);
    }

    // this function will be run from the EDT
    private static void createAndShowGUI() throws Exception
    {
        HeatMapFrame hmf = new HeatMapFrame();
        hmf.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        hmf.setSize(500,500);
        hmf.setVisible(true);
    }

    public static void main(String[] args)
    {
        SwingUtilities.invokeLater(new Runnable()
        {
            public void run()
            {
                try
                {
                    createAndShowGUI();
                }
                catch (Exception e)
                {
                    System.err.println(e);
                    e.printStackTrace();
                }
            }
        });
    }

}
