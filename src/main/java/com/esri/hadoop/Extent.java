package com.esri.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Extent - Minimum Bounding Box
 */
public class Extent implements Writable
{
    public double xmin;
    public double ymin;
    public double xmax;
    public double ymax;

    public static final Extent NULL_EXTENT = new Extent(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);

    /**
     * Default constructor
     */
    public Extent()
    {
    }

    /**
     * Creates and extent
     *
     * @param xmin minimum x
     * @param ymin minimum y
     * @param xmax maximum x
     * @param ymax maximum y
     */
    public Extent(
            double xmin,
            double ymin,
            double xmax,
            double ymax)
    {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof Extent))
        {
            return false;
        }

        final Extent extent = (Extent) o;

        if (Double.compare(extent.xmax, xmax) != 0)
        {
            return false;
        }
        if (Double.compare(extent.xmin, xmin) != 0)
        {
            return false;
        }
        if (Double.compare(extent.ymax, ymax) != 0)
        {
            return false;
        }
        if (Double.compare(extent.ymin, ymin) != 0)
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
        temp = Double.doubleToLongBits(xmin);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(ymin);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(xmax);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(ymax);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public boolean isEqual(
            final Extent extent,
            final double tolerance)
    {
        return Math.abs(extent.xmin - xmin) <= tolerance &&
                Math.abs(extent.ymin - ymin) <= tolerance &&
                Math.abs(extent.xmax - xmax) <= tolerance &&
                Math.abs(extent.ymax - ymax) <= tolerance;
    }

    /**
     * Sets the extent with values from that extent
     *
     * @param thatExtent the extent
     */
    public void set(final Extent thatExtent)
    {
        this.xmin = thatExtent.xmin;
        this.ymin = thatExtent.ymin;
        this.xmax = thatExtent.xmax;
        this.ymax = thatExtent.ymax;
    }

    /**
     * Sets the extent with these values in place
     *
     * @param xmin the minimum x value
     * @param ymin the minimum y value
     * @param xmax the maximum x value
     * @param ymax the maximumy value
     */
    public void set(
            final double xmin,
            final double ymin,
            final double xmax,
            final double ymax)
    {
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
    }

    /**
     * Creates a clone of the extent
     *
     * @return a new extent
     */
    public Extent clone()
    {
        return new Extent(xmin, ymin, xmax, ymax);
    }

    /**
     * Calculates the area of the extent
     *
     * @return the area
     */
    public double area()
    {
        return width() * height();
    }

    /**
     * Calculates if this extent is disjoint from that extent
     *
     * @param thatExtent
     * @return
     */
    public boolean isDisjoint(final Extent thatExtent)
    {
        return thatExtent.xmax < xmin ||
                thatExtent.xmin > xmax ||
                thatExtent.ymax < ymin ||
                thatExtent.ymin > ymax;
    }

    /**
     * Creates a new extent from the union of this extent and that extent
     *
     * @param thatExtent
     * @return
     */
    public Extent union(final Extent thatExtent)
    {
        final Extent copy = clone();
        copy.unionInPlace(thatExtent);
        return copy;
    }

    /**
     * Computes union in place from this extent and that extent
     *
     * @param thatExtent
     */
    public void unionInPlace(final Extent thatExtent)
    {
        if (thatExtent.xmin < xmin)
        {
            xmin = thatExtent.xmin;
        }
        if (thatExtent.xmax > xmax)
        {
            xmax = thatExtent.xmax;
        }
        if (thatExtent.ymin < ymin)
        {
            ymin = thatExtent.ymin;
        }
        if (thatExtent.ymax > ymax)
        {
            ymax = thatExtent.ymax;
        }
    }

    /**
     * Contains or touches, no error
     *
     * @param x x ordinate
     * @param y y ordinate
     * @return
     */
    public boolean containsPoint(
            final double x,
            final double y)
    {
        return xmin <= x && ymin <= y && x <= xmax && y <= ymax;
    }

    /**
     * Extent width
     *
     * @return the width
     */
    public double width()
    {
        return xmax - xmin;
    }

    /**
     * Extent height
     *
     * @return the height
     */
    public double height()
    {
        return ymax - ymin;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(xmin);
        dataOutput.writeDouble(ymin);
        dataOutput.writeDouble(xmax);
        dataOutput.writeDouble(ymax);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        xmin = dataInput.readDouble();
        ymin = dataInput.readDouble();
        xmax = dataInput.readDouble();
        ymax = dataInput.readDouble();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Extent{");
        sb.append("xmin=").append(xmin);
        sb.append(", ymin=").append(ymin);
        sb.append(", xmax=").append(xmax);
        sb.append(", ymax=").append(ymax);
        sb.append('}');
        return sb.toString();
    }
}
