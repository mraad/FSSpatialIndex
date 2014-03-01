package com.esri.hadoop.quadtree;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Point data
 */
public class PointData implements Writable
{
    /**
     * The x value
     */
    public double x;

    /**
     * The y value
     */
    public double y;

    /**
     * The handle to the point record
     */
    public long address;

    /**
     * Ctor
     */
    public PointData()
    {
    }

    /**
     * Constructs one of these
     *
     * @param x       the x value
     * @param y       the y value
     * @param address the handle
     */
    public PointData(
            final double x,
            final double y,
            final long address)
    {
        this.x = x;
        this.y = y;
        this.address = address;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
        dataOutput.writeLong(address);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        x = dataInput.readDouble();
        y = dataInput.readDouble();
        address = dataInput.readLong();
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof PointData))
        {
            return false;
        }

        final PointData pointData = (PointData) o;

        if (address != pointData.address)
        {
            return false;
        }
        if (Double.compare(pointData.x, x) != 0)
        {
            return false;
        }
        if (Double.compare(pointData.y, y) != 0)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        temp = Double.doubleToLongBits(x);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(y);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (address ^ (address >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("PointData{");
        sb.append("x=").append(x);
        sb.append(", y=").append(y);
        sb.append(", address=").append(address);
        sb.append('}');
        return sb.toString();
    }
}
