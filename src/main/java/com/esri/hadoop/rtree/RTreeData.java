package com.esri.hadoop.rtree;

import com.esri.hadoop.Extent;

/**
 * This is data within a RTree Node
 * Data can be either and MBR with a handle,
 * or another RTree Node
 */
abstract public class RTreeData {

    /**
     * Returns the extent of the data
     * @return the extent
     */
    abstract public Extent getExtent();

    /**
     * The handle of either the MBR or the handle of another Node
     * @return the handle
     */
    abstract public long getHandle();
}
