package com.spotify.heroic.http.render;

/**
 * Created by lucile on 16/05/17.
 */
public class Rounder {
    //utility array
    protected static final double factors[] =
            {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

    /**
     * Round the double <n> to <places> decimal places.  If <places> is negative or greater than 9,
     * round to 9 decimal places
     * @param n
     * @param places
     * @return
     */
    public static double round(double n, int places)
    {
        if (places < 0 || places > 9) places = 9;

        return Math.round(n * factors[places]) / factors[places];
    }
}
