package com.esri.hadoop.quadtree;

/**
 * This search function is used when searching.  Its the callback.
 * Used when you call search on the reader.
 */
public interface IEvaluateFunction
{
    /**
     * This allows you to create your own function as a callback during search.
     *
     * @param pointData the data point (x,y,address)
     */
    public void evaluate(final PointData pointData);
}
