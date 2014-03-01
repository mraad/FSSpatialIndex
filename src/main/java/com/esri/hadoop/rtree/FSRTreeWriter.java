package com.esri.hadoop.rtree;


import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * FSRTreeWriter allows you to write a R tree to a stream, thus enabling you to spatially index.
 * Create one, add multiple points and  then close.
 */
public class FSRTreeWriter
{
    private RTree m_rTree;
    private FSDataOutputStream m_stream;


    /**
     * Create a FSRTreeWriter
     *
     * @param stream       the output stream
     * @param nodeLowSize  the "minimum" number of entries, not really though
     * @param nodeHighSize the maximum number of entries in a node
     *                     <p/>
     *                     NOTE: NodeHighSize is important.  The number of mbrs on each node
     *                     Make it like 50 or so.  Well, maybe 25...dependent on the application
     */
    public FSRTreeWriter(
            final FSDataOutputStream stream,
            final int nodeLowSize,
            final int nodeHighSize)
    {
        m_stream = stream;
        m_rTree = new RTree(nodeLowSize, nodeHighSize);
    }

    /**
     * Add an entry
     *
     * @param data an extent with a handle
     */
    public void add(MBRHandle data)
    {
        m_rTree.insert(data);
    }

    /**
     * Done adding points; write then, shut down and release
     */
    public void close()
    {
        try
        {
            m_rTree.write(m_stream);
        }
        catch (IOException e)
        {
            e.printStackTrace();//quad tree writing problem
        }
        finally
        {
            m_rTree.release();
            try
            {
                m_stream.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();//stream problem
            }
        }
    }
}
