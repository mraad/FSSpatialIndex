package com.esri.hadoop.rtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataInputStream;

import java.util.Iterator;
import java.util.Stack;

/**
 * RTreeIterator is used to search over the rtree,
 * in conjunction with an input stream
 */
public class RTreeIterator implements Iterator<MBRHandle>
{

    private FSDataInputStream m_stream;
    private Extent m_extent;
    private Stack<Long> m_nodeHandles;
    private Stack<MBRHandle> m_dataHandles;
    private RTree m_rTree;

    /**
     * Construct a search iterator
     *
     * @param stream the input stream
     * @param extent the extent being searched
     */
    public RTreeIterator(
            final RTree rTree,
            final FSDataInputStream stream,
            final Extent extent)
    {
        m_stream = stream;
        m_extent = extent;
        m_nodeHandles = new Stack<Long>();
        m_dataHandles = new Stack<MBRHandle>();
        m_rTree = rTree;
    }

    @Override
    public boolean hasNext()
    {
        if (!m_dataHandles.isEmpty())
        {
            return true;
        }
        if (m_nodeHandles.isEmpty())
        {
            return false;
        }
        final long handle = m_nodeHandles.pop();
        try
        {
            m_rTree.searchNode(this, m_stream, m_extent, handle);
        }
        finally
        {
            return hasNext();
        }
    }

    @Override
    public MBRHandle next()
    {
        // if (hasNext())
        // {
        return m_dataHandles.pop();
        // }
        // return null;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * This is used internally.  Its called from RTree and adds a MBRHandle to the queue.
     * The iterator emptys the queue before proceeding to the next node
     *
     * @param mbrHandle
     */
    public void addMBRHandle(MBRHandle mbrHandle)
    {
        m_dataHandles.push(mbrHandle);
    }

    /**
     * This is used internally. Its called from RTree and adds a handle to a node (long)
     * The dataHandle queue is emptied first.
     *
     * @param nodeHandle
     */
    public void addNodeHandle(long nodeHandle)
    {
        m_nodeHandles.push(nodeHandle);
    }

}
