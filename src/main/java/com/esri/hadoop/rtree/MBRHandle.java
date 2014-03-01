package com.esri.hadoop.rtree;

import com.esri.hadoop.Extent;

/**
 * The extent and handle
 */
public class MBRHandle extends RTreeData
{

    /**
     * The handle to the record
     */
    public long handle;

    /**
     * The MBR
     */
    public Extent extent;

    /**
     * Constructs a data handle
     *
     * @param extent the extent
     * @param handle the handle
     */
    public MBRHandle(
            Extent extent,
            long handle)
    {
        this.extent = extent;
        this.handle = handle;
    }

    /**
     * Retrieves the extent
     *
     * @return the MBR of the data
     */
    public Extent getExtent()
    {
        return extent;
    }

    /**
     * The handle to the record
     *
     * @return the handle, aka seek address
     */
    public long getHandle()
    {
        return handle;
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof MBRHandle))
        {
            return false;
        }
        MBRHandle handle = (MBRHandle) obj;
        return (handle.handle == this.handle);
    }
}
