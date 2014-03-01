package com.esri.hadoop.rtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Iterator;

/**
 * FSRTreeReader allows you to search over a stream, create one, search as many times
 * as you like and release when done.
 */
public class FSRTreeReader
{

    private RTree m_rTree;
    private FSDataInputStream m_stream;

    /**
     * Create a FSRTreeReader
     *
     * @param stream the input stream
     */
    public FSRTreeReader(final FSDataInputStream stream) throws IOException
    {
        m_stream = stream;
        m_rTree = new RTree(stream);
    }

    /**
     * Performs a extent search over the entire tree and then executes the method
     * searchFunction(PointData) on all data in the extent (contained or touch)
     */
    public Iterator<MBRHandle> search(final Extent extent) throws IOException
    {
        return m_rTree.search(extent);
    }
}
