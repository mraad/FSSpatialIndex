package com.esri.hadoop.quadtree;

import com.esri.hadoop.Extent;
import com.esri.hadoop.MiniFS;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by kcoffin on 2/6/14.
 */
public class HadoopQuadTreeTest extends MiniFS
{

    public static final double TINY = 1e-10;

    @Test
    public void testUpperLeftAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(-20, 60, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        final DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to upper left", -50, dfs.x, TINY);
        assertEquals("Point not added correctly to upper left", 50, dfs.y, TINY);
        assertEquals("Point not added correctly to upper left", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to upper left", 18, dfs.level);
    }

    @Test
    public void testUpperAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(20, 60, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        final DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to upper", 0, dfs.x, TINY);
        assertEquals("Point not added correctly to upper", 50, dfs.y, TINY);
        assertEquals("Point not added correctly to upper", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to upper", 18, dfs.level);
    }

    @Test
    public void testUpperRightAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(60, 60, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        final DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to upper right", 50, dfs.x, TINY);
        assertEquals("Point not added correctly to upper right", 50, dfs.y, TINY);
        assertEquals("Point not added correctly to upper right", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to upper right", 18, dfs.level);
    }

    @Test
    public void testLowerLeftAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(-20, -20, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        final DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to Lower left", -50, dfs.x, TINY);
        assertEquals("Point not added correctly to Lower left", -50, dfs.y, TINY);
        assertEquals("Point not added correctly to Lower left", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to Lower left", 18, dfs.level);
    }

    @Test
    public void testLowerAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(20, -10, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        final DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to Lower", 0, dfs.x, TINY);
        assertEquals("Point not added correctly to Lower", -50, dfs.y, TINY);
        assertEquals("Point not added correctly to Lower", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to Lower", 18, dfs.level);
    }

    @Test
    public void testLowerRightAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(60, -10, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        final DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to lower Right", 50, dfs.x, TINY);
        assertEquals("Point not added correctly to lower Right", -50, dfs.y, TINY);
        assertEquals("Point not added correctly to lower Right", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to lower Right", 18, dfs.level);
    }

    @Test
    public void testRightAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(60, 10, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to Right", 50, dfs.x, TINY);
        assertEquals("Point not added correctly to Right", 0, dfs.y, TINY);
        assertEquals("Point not added correctly to Right", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to Right", 18, dfs.level);
    }

    @Test
    public void testLeftAdd() throws Exception
    {
        openOutputStream();
        FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(-20, 20, 30));
        writer.close();

        openInputStream();
        FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        DepthFirstSearch dfs = new DepthFirstSearch();
        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to  left", -50, dfs.x, TINY);
        assertEquals("Point not added correctly to  left", 0, dfs.y, TINY);
        assertEquals("Point not added correctly to  left", 50, dfs.width, TINY);
        assertEquals("Point not added correctly to  left", 18, dfs.level);
    }

    @Test
    public void testLeftLeftAdd() throws Exception
    {
        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 2, new Extent(0, 0, 50, 50));
        writer.addPointData(new PointData(-60, 20, 30));
        writer.close();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);
        final DepthFirstSearch dfs = new DepthFirstSearch();

        reader.depthFirstSearch(dfs);
        assertEquals("Point not added correctly to left left", -150, dfs.x, TINY);
        assertEquals("Point not added correctly to left left", 0, dfs.y, TINY);
        assertEquals("Point not added correctly to left left", 100, dfs.width, TINY);
        assertEquals("Point not added correctly to left left", 19, dfs.level);
    }

    @Test
    public void testSearch() throws Exception
    {
        final int N = 10000;

        openOutputStream();
        final FSQuadTreeWriter writer = new FSQuadTreeWriter(m_dataOutputStream, 128, new Extent(0, 0, 50, 50));
        for (int i = 0; i < N; i++)
        {
            writer.addPointData(new PointData(Math.random() * 100, Math.random() * 100, (long) Math.random() * 100));
        }
        writer.addPointData(new PointData(20, 20, 30));
        writer.addPointData(new PointData(80, 20, 80));

        long t1 = System.currentTimeMillis();
        writer.close();
        long t2 = System.currentTimeMillis();

        openInputStream();
        final FSQuadTreeReader reader = new FSQuadTreeReader(m_dataInputStream);

        long t3 = System.currentTimeMillis();
        final EvaluateFunction evalFunc = new EvaluateFunction();
        reader.search(new Extent(0, 0, 15, 5), evalFunc);
        long t4 = System.currentTimeMillis();

        final Iterator iterator = reader.search(new Extent(0, 0, 15, 5));
        int count = 0;
        while (iterator.hasNext())
        {
            iterator.next();
            count++;
        }
        long t5 = System.currentTimeMillis();

        //System.out.println("Largest Node size: "+((SearchIterator)iterator).largestNodeSize);
        //System.out.println("Largest Point size: "+((SearchIterator)iterator).largestPointSize);

        System.out.println("Time to write " + N + " Points (ms) = " + (t2 - t1));
        System.out.println("Time to search " + N + " Points (ms) = " + (t4 - t3));
        // System.out.println("Found " + evalFunc.count + "  Points.");
        System.out.println("Time to Iterator search " + N + " Points (ms) = " + (t5 - t4));
        // System.out.println("Iterator Found " + count + "  Points.");

        assertEquals(evalFunc.count, count);
        //reader.DFS(new printDFS());
    }

    private final class EvaluateFunction implements IEvaluateFunction
    {
        public int count = 0;

        @Override
        public void evaluate(PointData pointData)
        {
            count++;
        }
    }

    public final class DepthFirstSearch implements INodeFunction
    {
        public double x;
        public double y;
        public double width;
        public int level;

        public void evaluate(
                final QuadTreeNode node,
                final double lowerLeftX,
                final double lowerLeftY,
                final double nodeWidth,
                final int level)
        {
            int N = 0;
            if (node.getData() != null)
            {
                N = node.getData().size();
                if (N == 1)
                {
                    this.x = lowerLeftX;
                    this.y = lowerLeftY;
                    this.width = nodeWidth;
                    this.level = level;
                }
            }
        }
    }


}
