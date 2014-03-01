package com.esri.hadoop.quadtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Quad Tree Node
 */
public class QuadTreeNode
{
    private List<PointData> m_data;
    private QuadTreeNode[] m_children;
    private long[] m_seekTo;
    private long m_recordSize;
    private long m_recordSizeBeneath;

    /**
     * Clear and releases this quad tree instance.
     */
    public void clear()
    {
        if (m_children != null)
        {
            for (final QuadTreeNode child : m_children)
            {
                child.clear();
            }
        }
        if (m_data != null)
        {
            m_data.clear();
        }
    }

    /**
     * Clear and releases this quad tree instance
     */
    public void clearThisNode()
    {
        m_children = null;
        if (m_data != null)
        {
            m_data.clear();
        }
    }

    /**
     * Reads the node from stream
     * Does not instantiate children of this node, but has the seek addresses to where they are at.
     *
     * @param dataInputStream the input stream
     * @throws IOException
     */
    public void read(final FSDataInputStream dataInputStream) throws IOException
    {
        final int dataSize = dataInputStream.readInt();
        for (int i = 0; i < dataSize; i++)
        {
            final double x = dataInputStream.readDouble();
            final double y = dataInputStream.readDouble();
            final long address = dataInputStream.readLong();
            m_data.add(new PointData(x, y, address));
        }
        final boolean hasChildren = dataInputStream.readBoolean();
        if (hasChildren)
        {
            m_seekTo = new long[4];
            for (int i = 0; i < 4; i++)
            {
                m_seekTo[i] = dataInputStream.readLong();
            }
            final long handle = dataInputStream.getPos();

            m_seekTo[3] = m_seekTo[2] + m_seekTo[1] + m_seekTo[0] + handle;
            m_seekTo[2] = m_seekTo[1] + m_seekTo[0] + handle;
            m_seekTo[1] = m_seekTo[0] + handle;
            m_seekTo[0] = handle;
        }
        else
        {
            m_children = null;
            m_seekTo = null;
        }
    }

    /**
     * The record size of this node
     *
     * @return
     */
    public long recordSize()
    {
        long n = 0;
        if (m_children == null)
        {
            n += 1;
        }
        else
        {
            n += 33;
        }
        if (m_data == null || m_data.size() == 0)
        {
            n += 4;
        }
        else
        {
            n += 4 + m_data.size() * 24;
        }
        return n;
    }

    /**
     * The total record size of this node and those below
     *
     * @return
     */
    public long totalRecordSize()
    {
        return m_recordSizeBeneath + m_recordSize;
    }

    /**
     * Calculates all the sizes before writing
     */
    public void calculateTotalRecordSize()
    {
        if (m_children != null)
        {
            for (QuadTreeNode node : m_children)
            {
                node.calculateTotalRecordSize();
            }
        }
        m_recordSize = recordSize();
        m_recordSizeBeneath = 0;
        if (m_children != null)
        {
            for (QuadTreeNode node : m_children)
            {
                m_recordSizeBeneath += node.totalRecordSize();
            }
        }
    }

    /**
     * Writes out this node
     *
     * @param dataOutputStream
     * @throws IOException
     */
    public void write(final FSDataOutputStream dataOutputStream) throws IOException
    {
        if (m_data == null || m_data.size() == 0)
        {
            dataOutputStream.writeInt(0);
        }
        else
        {
            dataOutputStream.writeInt(m_data.size());
            for (final PointData pt : m_data)
            {
                dataOutputStream.writeDouble(pt.x);
                dataOutputStream.writeDouble(pt.y);
                dataOutputStream.writeLong(pt.address);
            }
        }
        if (m_children == null)
        {
            dataOutputStream.writeBoolean(false);
        }
        else
        {
            dataOutputStream.writeBoolean(true);
            for (final QuadTreeNode node : m_children)
            {
                dataOutputStream.writeLong(node.totalRecordSize());
            }
            for (final QuadTreeNode node : m_children)
            {
                node.write(dataOutputStream);
            }
        }
    }

    /**
     * Constructs a node
     */
    public QuadTreeNode()
    {
        m_data = new ArrayList<PointData>();
        m_children = null;
        m_seekTo = null;
    }

    /**
     * Retrieves the children of this node if any (if not null)
     */
    public QuadTreeNode[] getChildren()
    {
        return m_children;
    }

    /**
     * Sets the children of this node
     */
    public void setChildren(QuadTreeNode[] children)
    {
        m_children = children;
    }

    /**
     * Retrieves the vector of points at this node
     */
    public List<PointData> getData()
    {
        return m_data;
    }

    /**
     * Sets the vector points at this node
     */
    public void setData(ArrayList<PointData> data)
    {
        m_data = data;
    }

    /**
     * Depth first search
     */
    public void depthFirstSeach(
            final FSDataInputStream stream,
            final INodeFunction nodeFunction,
            final double x,
            final double y,
            final double width,
            final int level) throws IOException
    {
        nodeFunction.evaluate(this, x, y, width, level);
        final double ww = width * 0.5;
        checkIfLoaded(stream);
        if (m_children != null)
        {
            m_children[0].depthFirstSeach(stream, nodeFunction, x, y, ww, level + 1);
            m_children[1].depthFirstSeach(stream, nodeFunction, x, y + ww, ww, level + 1);
            m_children[2].depthFirstSeach(stream, nodeFunction, x + ww, y, ww, level + 1);
            m_children[3].depthFirstSeach(stream, nodeFunction, x + ww, y + ww, ww, level + 1);
        }
    }

    /**
     * Check if the node is laoded, and if not loads it.
     *
     * @param stream
     * @throws IOException
     */
    public void checkIfLoaded(final FSDataInputStream stream) throws IOException
    {
        if (m_children == null && m_seekTo != null)
        {
            m_children = new QuadTreeNode[4];
            for (int i = 0; i < 4; i++)
            {
                m_children[i] = new QuadTreeNode();
                stream.seek(m_seekTo[i]);
                m_children[i].read(stream);
            }
        }
    }

    /**
     * Searches an extent on a node
     */
    public void search(
            final SearchIterator iterator,
            final Extent extent,
            final FSDataInputStream dataInputStream,
            final double x,
            final double y,
            final double width) throws IOException
    {
        //the extent being searched for and this node do intersect
        if (extent.xmin < x + width && extent.xmax >= x && extent.ymax >= y && extent.ymin < y + width)
        {
            checkIfLoaded(dataInputStream);
            if (m_children == null)
            {
                //the extent being searched over fully contains this node
                if (extent.xmin <= x && extent.ymin <= y && extent.xmax > x + width && extent.ymax > y + width)
                {
                    for (final PointData pt : m_data)
                    {
                        iterator.addPoint(pt);
                    }
                }
                else
                {
                    for (final PointData pt : m_data)
                    {
                        if (extent.containsPoint(pt.x, pt.y))
                        {
                            iterator.addPoint(pt);
                        }
                    }
                }
            }
            else
            {
                final double ww = width * 0.5;
                iterator.addChild(m_children[0], x, y, ww);
                iterator.addChild(m_children[1], x, y + ww, ww);
                iterator.addChild(m_children[2], x + ww, y, ww);
                iterator.addChild(m_children[3], x + ww, y + ww, ww);
            }
        }
    }

    /**
     * Adds a point on a node
     */
    public void addPoint(
            final QuadTree qt,
            final PointData pt,
            final double x,
            final double y,
            final double width,
            final int level)
    {
        final double ww = width * 0.5;
        final double xx = x + ww;
        final double yy = y + ww;

        if (m_data != null)
        {
            m_data.add(pt);

            if ((m_data.size() > qt.bucketSize()) && (level > qt.minimumLevel()))
            {
                //trace(level);
                m_children = new QuadTreeNode[4];

                m_children[0] = new QuadTreeNode();
                m_children[1] = new QuadTreeNode();
                m_children[2] = new QuadTreeNode();
                m_children[3] = new QuadTreeNode();
                for (PointData pnt : m_data)
                {
                    if (pnt.x < xx)
                    {
                        if (pnt.y < yy)
                        {
                            m_children[0].addPoint(qt, pnt, x, y, ww, level - 1);
                        }
                        else
                        {
                            m_children[1].addPoint(qt, pnt, x, yy, ww, level - 1);
                        }
                    }
                    else
                    {
                        if (pnt.y < yy)
                        {
                            m_children[2].addPoint(qt, pnt, xx, y, ww, level - 1);
                        }
                        else
                        {
                            m_children[3].addPoint(qt, pnt, xx, yy, ww, level - 1);
                        }
                    }
                }
                m_data = null;
            }
        }
        else
        {
            if (pt.x < xx)
            {
                if (pt.y < yy)
                {
                    m_children[0].addPoint(qt, pt, x, y, ww, level - 1);
                }
                else
                {
                    m_children[1].addPoint(qt, pt, x, yy, ww, level - 1);
                }
            }
            else
            {
                if (pt.y < yy)
                {
                    m_children[2].addPoint(qt, pt, xx, y, ww, level - 1);
                }
                else
                {
                    m_children[3].addPoint(qt, pt, xx, yy, ww, level - 1);
                }
            }
        }
    }
}
