package com.esri.hadoop.rtree;

import com.esri.hadoop.Extent;
import com.esri.hadoop.MiniFS;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by kcoffin on 2/13/14.
 */
@Ignore
public class HadoopRTreeTest extends MiniFS
{
    @Test
    public void testSearch() throws Exception
    {
        openOutputStream();

        final FSRTreeWriter writer = new FSRTreeWriter(m_dataOutputStream, 2, 4);

        writer.add(new MBRHandle(new Extent(1, 0, 2, 2), 20L));
        writer.add(new MBRHandle(new Extent(3, 3, 10, 10), 10L));
        writer.add(new MBRHandle(new Extent(3, 3, 6, 6), 50L));
        writer.add(new MBRHandle(new Extent(0, 1, 3, 3), 30L));
        writer.add(new MBRHandle(new Extent(1, 1, 2, 2), 40L));
        writer.add(new MBRHandle(new Extent(2, 2, 4, 4), 60L));
        writer.add(new MBRHandle(new Extent(1, 1, 2, 1), 70L));

        writer.close();

        final FSRTreeReader reader = new FSRTreeReader(m_dataInputStream);
        Iterator<MBRHandle> iterator = reader.search(new Extent(3.0001, 3.0001, 4, 4));
        while (iterator.hasNext())
        {
            MBRHandle handle = iterator.next();
            System.out.println("Extent: " + handle.extent + "  handle: " + handle.handle);
        }
    }

    @Test
    public void testBigSearch() throws Exception
    {
        openOutputStream();
        FSRTreeWriter writer = new FSRTreeWriter(m_dataOutputStream, 10, 20);
        Extent searchExtent = new Extent(53, 53, 54, 54);
        ArrayList<MBRHandle> badNumbers = new ArrayList<MBRHandle>();
        long N = 100000;
        int count = 0;
        for (long i = 0; i < N; i++)
        {
            double width = Math.random() * 30 + 1;
            double height = Math.random() * 30 + 1;
            double x = Math.random() * 100;
            double y = Math.random() * 100;
            Extent dataExtent = new Extent(x, y, x + width, y + height);
            MBRHandle handle = new MBRHandle(dataExtent, i);
            if (!dataExtent.isDisjoint(searchExtent))
            {
                count++;
                badNumbers.add(handle);

            }
            writer.add(handle);
        }

        writer.close();

        openInputStream();
        final FSRTreeReader reader = new FSRTreeReader(m_dataInputStream);
        final Iterator<MBRHandle> iterator = reader.search(searchExtent);
        int searchCount = 0;
        while (iterator.hasNext())
        {
            MBRHandle handle = iterator.next();
            //System.out.println("Extent: "+handle.extent+"  handle: "+handle.handle);
            searchCount++;
            badNumbers.remove(handle);
        }
        for (MBRHandle handle : badNumbers)
        {
            System.out.println("Not found: " + handle.extent);
        }
        System.out.println("Found " + searchCount + " matches.");
        assertEquals("Search has failed", count, searchCount);
        assertEquals("Search has failed", 0, badNumbers.size());

    }
}
