package com.esri.hadoop;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Extent Unit Tests
 */
public class ExtentTest
{

    public static final double TINY = 1e-10;

    @Test
    public void testSet() throws Exception
    {
        Extent extent = new Extent();
        extent.set(1, 2, 3, 4);
        assertEquals("xmin not correct", 1, extent.xmin, TINY);
        assertEquals("xmin not correct", 2, extent.ymin, TINY);
        assertEquals("xmin not correct", 3, extent.xmax, TINY);
        assertEquals("xmin not correct", 4, extent.ymax, TINY);
    }

    @Test
    public void testSet1() throws Exception
    {
        Extent extent = new Extent();
        extent.set(new Extent(1, 2, 3, 4));
        assertEquals("xmin not correct", 1, extent.xmin, TINY);
        assertEquals("xmin not correct", 2, extent.ymin, TINY);
        assertEquals("xmin not correct", 3, extent.xmax, TINY);
        assertEquals("xmin not correct", 4, extent.ymax, TINY);
    }

    @Test
    public void testClone() throws Exception
    {
        Extent extent1 = new Extent();
        extent1.set(1, 2, 3, 4);
        Extent extent = extent1.clone();
        assertEquals("xmin not correct", 1, extent.xmin, TINY);
        assertEquals("ymin not correct", 2, extent.ymin, TINY);
        assertEquals("xmax not correct", 3, extent.xmax, TINY);
        assertEquals("ymax not correct", 4, extent.ymax, TINY);
    }

    @Test
    public void testArea() throws Exception
    {
        Extent extent = new Extent();
        extent.set(1, 2, 3, 4);
        assertEquals("area not correct", 4, extent.area(), TINY);
    }

    @Test
    public void testIsDisjoint() throws Exception
    {
        Extent extent1 = new Extent(5, 5, 6, 6);
        Extent extent2 = new Extent(7, 7, 8, 8);
        Extent extent3 = new Extent(4, 4, 5.5, 5.5);
        assertEquals("isDisjoint not correct", true, extent1.isDisjoint(extent2));
        assertEquals("isDisjoint not correct", false, extent1.isDisjoint(extent3));
    }

    @Test
    public void testUnion() throws Exception
    {
        Extent extent1 = new Extent(1, 2, 3, 4);

        Extent extent = extent1.union(new Extent(0, 1, 0, 1));
        assertEquals("Union-xmin not correct", 0, extent.xmin, TINY);
        assertEquals("Union-ymin not correct", 1, extent.ymin, TINY);
        assertEquals("Union-xmax not correct", 3, extent.xmax, TINY);
        assertEquals("Union-ymax not correct", 4, extent.ymax, TINY);

        extent = extent1.union(new Extent(-1, -2, 10, 10));
        assertEquals("Union-xmin not correct", -1, extent.xmin, TINY);
        assertEquals("Union-ymin not correct", -2, extent.ymin, TINY);
        assertEquals("Union-xmax not correct", 10, extent.xmax, TINY);
        assertEquals("Union-ymax not correct", 10, extent.ymax, TINY);
    }

    @Test
    public void testUnionInPlace() throws Exception
    {
        Extent extent = new Extent(1, 2, 3, 4);

        extent.unionInPlace(new Extent(0, 1, 0, 1));
        assertEquals("union in place-xmin not correct", 0, extent.xmin, TINY);
        assertEquals("union in place-ymin not correct", 1, extent.ymin, TINY);
        assertEquals("union in place-xmax not correct", 3, extent.xmax, TINY);
        assertEquals("union in place-ymax not correct", 4, extent.ymax, TINY);

        extent.unionInPlace(new Extent(-1, -2, 10, 10));
        assertEquals("union in place-xmin not correct", -1, extent.xmin, TINY);
        assertEquals("union in place-ymin not correct", -2, extent.ymin, TINY);
        assertEquals("union in place-xmax not correct", 10, extent.xmax, TINY);
        assertEquals("union in place-ymax not correct", 10, extent.ymax, TINY);
    }

    @Test
    public void testContainsPoint() throws Exception
    {
        Extent extent = new Extent();
        extent.set(1, 2, 3, 4);

        assertEquals("width not correct", true, extent.containsPoint(2, 3));
        assertEquals("width not correct", false, extent.containsPoint(4, 5));
    }

    @Test
    public void testWidth() throws Exception
    {
        Extent extent = new Extent();
        extent.set(1, 2, 3, 4);
        assertEquals("width not correct", 2, extent.width(), TINY);
    }

    @Test
    public void testHeight() throws Exception
    {
        Extent extent = new Extent();
        extent.set(1, 2, 3, 4);
        assertEquals("height not correct", 2, extent.height(), TINY);
    }
}
