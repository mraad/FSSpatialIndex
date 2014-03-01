package com.esri.hadoop.quadtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Quad tree that can grow up and down.
 */
public class QuadTree
{
    //the root node
    private QuadTreeNode m_root;

    //the bucket size
    private int m_bucketSize;

    //the root level
    private int m_rootLevel;

    //the maximum level
    private int m_maximumLevel;

    //the minimum level
    private int m_minimumLevel;

    //the root width
    private double m_rootWidth;

    //the root node's minx,miny
    private Point2D.Double m_rootMin;

    //the root node's maxx, maxy
    private Point2D.Double m_rootMax;

    //the overflow
    private List<PointData> m_overflow = new ArrayList<PointData>();

    public void release()
    {
        m_root.clear();
        m_overflow.clear();
    }

    /**
     * Constructs a quad tree from a dataInputStream
     *
     * @param dataInputStream the data input stream
     * @throws IOException
     */
    public QuadTree(final FSDataInputStream dataInputStream) throws IOException
    {
        m_bucketSize = dataInputStream.readInt();
        m_rootLevel = dataInputStream.readInt();
        m_maximumLevel = dataInputStream.readInt();
        m_minimumLevel = dataInputStream.readInt();
        m_rootWidth = dataInputStream.readDouble();

        double x1 = dataInputStream.readDouble();
        double y1 = dataInputStream.readDouble();
        m_rootMin = new Point2D.Double(x1, y1);
        double x2 = dataInputStream.readDouble();
        double y2 = dataInputStream.readDouble();
        m_rootMax = new Point2D.Double(x2, y2);

        //overflow
        int n = dataInputStream.readInt();
        for (int i = 0; i < n; i++)
        {
            double x = dataInputStream.readDouble();
            double y = dataInputStream.readDouble();
            long address = dataInputStream.readLong();
            m_overflow.add(new PointData(x, y, address));
        }

        m_root = new QuadTreeNode();
        m_root.read(dataInputStream);
    }

    /**
     * Writes to stream
     *
     * @param dataOutputStream
     * @throws IOException
     */
    public void write(final FSDataOutputStream dataOutputStream) throws IOException
    {
        m_root.calculateTotalRecordSize();

        dataOutputStream.writeInt(m_bucketSize);
        dataOutputStream.writeInt(m_rootLevel);
        dataOutputStream.writeInt(m_maximumLevel);
        dataOutputStream.writeInt(m_minimumLevel);
        dataOutputStream.writeDouble(m_rootWidth);

        dataOutputStream.writeDouble(m_rootMin.x);
        dataOutputStream.writeDouble(m_rootMin.y);
        dataOutputStream.writeDouble(m_rootMax.x);
        dataOutputStream.writeDouble(m_rootMax.y);

        //overflow
        dataOutputStream.writeInt(m_overflow.size());
        for (PointData pt : m_overflow)
        {
            dataOutputStream.writeDouble(pt.x);
            dataOutputStream.writeDouble(pt.y);
            dataOutputStream.writeLong(pt.address);
        }

        //root node

        m_root.write(dataOutputStream);
    }

    /**
     * The ctor for this quad tree
     *
     * @param bucketSize   the decomposition bucket size (exceed this size and the bucket is subdivided)
     * @param startLevel   the start level
     * @param minimumLevel the minimum level allowed
     * @param maximumLevel the maximum level allowed
     * @param startWidth   the startWidth
     * @param minPoint     the minx,miny of the starting root node
     */
    public QuadTree(
            final int bucketSize,
            final int startLevel,
            final int minimumLevel,
            final int maximumLevel,
            final double startWidth,
            final Point2D.Double minPoint)
    {
        m_bucketSize = bucketSize;
        m_rootLevel = startLevel;
        m_minimumLevel = minimumLevel;
        m_maximumLevel = maximumLevel;
        m_rootWidth = startWidth;
        m_rootMin = new Point2D.Double(minPoint.x, minPoint.y);
        m_rootMax = new Point2D.Double(minPoint.x + m_rootWidth, minPoint.y + m_rootWidth);
        m_root = new QuadTreeNode();
    }

    /**
     * Conducts a Depth First Search on the quad tree.
     * Ultimately this executes nodeFunction(UnionPointQuadTreeNode, xminForNode,yminForNode, width) for each node
     */
    public void depthSearchFirst(
            final FSDataInputStream stream,
            final INodeFunction nodeFunction) throws IOException
    {
        m_root.depthFirstSeach(stream, nodeFunction, m_rootMin.x, m_rootMin.y, m_rootWidth, m_rootLevel);
    }

    /**
     * The bucket size
     *
     * @return
     */
    public int bucketSize()
    {
        return m_bucketSize;
    }

    /**
     * The minimum level
     *
     * @return
     */
    public int minimumLevel()
    {
        return m_minimumLevel;
    }

    /**
     * Adds a point into the quad tree.
     */
    public void addPointData(final PointData pointData)
    {
        if (m_maximumLevel == m_rootLevel)
        {//all done growing out..just add to overflow
            if (pointData.x < m_rootMin.x || pointData.y < m_rootMin.y || pointData.x > m_rootMax.x || pointData.y > m_rootMax.y)
            {
                m_overflow.add(pointData);
            }
            else
            {
                //grow the quad tree in
                m_root.addPoint(this, pointData, m_rootMin.x, m_rootMin.y, m_rootWidth, m_rootLevel);
            }
            //first take care of growing the quad tree out (which is an odd case)
        }
        else
        {
            if (pointData.x < m_rootMin.x)
            {
                if (pointData.y < m_rootMin.y)
                {
                    final QuadTreeNode node = new QuadTreeNode();
                    final QuadTreeNode[] children = new QuadTreeNode[4];
                    children[0] = new QuadTreeNode();
                    children[1] = new QuadTreeNode();
                    children[2] = new QuadTreeNode();
                    children[3] = m_root;
                    node.setChildren(children);
                    node.setData(null);

                    m_root = node;
                    m_rootMin.x = m_rootMin.x - m_rootWidth;
                    m_rootMin.y = m_rootMin.y - m_rootWidth;
                    m_rootWidth *= 2.0;
                    m_rootLevel++;
                    addPointData(pointData);
                }
                else
                {
                    final QuadTreeNode node = new QuadTreeNode();
                    final QuadTreeNode[] children = new QuadTreeNode[4];
                    children[0] = new QuadTreeNode();
                    children[1] = new QuadTreeNode();
                    children[3] = new QuadTreeNode();
                    children[2] = m_root;
                    node.setChildren(children);
                    node.setData(null);

                    m_root = node;
                    m_rootMin.x = m_rootMin.x - m_rootWidth;
                    m_rootMax.y = m_rootMax.y + m_rootWidth;
                    m_rootWidth *= 2.0;
                    m_rootLevel++;
                    addPointData(pointData);
                }
            }
            else if (pointData.y < m_rootMin.y)
            {
                final QuadTreeNode node = new QuadTreeNode();
                final QuadTreeNode[] children = new QuadTreeNode[4];
                children[0] = new QuadTreeNode();
                children[3] = new QuadTreeNode();
                children[2] = new QuadTreeNode();
                children[1] = m_root;
                node.setChildren(children);
                node.setData(null);
                m_root = node;
                m_rootMax.x = m_rootMax.x + m_rootWidth;
                m_rootMin.y = m_rootMin.y - m_rootWidth;
                m_rootWidth *= 2.0;
                m_rootLevel++;
                addPointData(pointData);
            }
            else if (pointData.x > m_rootMax.x || pointData.y > m_rootMax.y)
            {
                final QuadTreeNode node = new QuadTreeNode();
                final QuadTreeNode[] children = new QuadTreeNode[4];
                children[3] = new QuadTreeNode();
                children[1] = new QuadTreeNode();
                children[2] = new QuadTreeNode();
                children[0] = m_root;
                node.setChildren(children);
                node.setData(null);
                m_root = node;
                m_rootMax.x = m_rootMax.x + m_rootWidth;
                m_rootMax.y = m_rootMax.y + m_rootWidth;
                m_rootWidth *= 2.0;
                m_rootLevel++;
                addPointData(pointData);
            }
            else
            {
                //grow the quad tree in
                m_root.addPoint(this, pointData, m_rootMin.x, m_rootMin.y, m_rootWidth, m_rootLevel);
            }
        }
    }

    /**
     * Performs a extent search over the entire quad tree and then executes the method
     * evaluateFunction(PointData) on all data in the extent (contained or touch)
     */
    public void search(
            final FSDataInputStream dataInputStream,
            final Extent extent,
            final IEvaluateFunction evaluateFunction) throws IOException
    {
        final Iterator<PointData> iterator = search(dataInputStream, extent);
        while (iterator.hasNext())
        {
            evaluateFunction.evaluate(iterator.next());
        }
    }

    /**
     * Searches quad tree by using the iterator
     *
     * @param dataInputStream the input dataInputStream
     * @param extent          the extent
     * @return the iterator.
     * @throws IOException
     */
    public Iterator<PointData> search(
            final FSDataInputStream dataInputStream,
            final Extent extent) throws IOException
    {
        final SearchIterator iterator = new SearchIterator(dataInputStream, extent);
        if (m_overflow.size() > 0 && (extent.xmax > m_rootMax.x || extent.ymax > m_rootMax.y ||
                extent.xmin < m_rootMin.x || extent.ymin < m_rootMin.y))
        {
            for (final PointData pt : m_overflow)
            {
                if (extent.containsPoint(pt.x, pt.y))
                {
                    iterator.addPoint(pt);
                }
            }
        }
        iterator.addChild(m_root, m_rootMin.x, m_rootMin.y, m_rootWidth);
        return iterator;
    }

}
