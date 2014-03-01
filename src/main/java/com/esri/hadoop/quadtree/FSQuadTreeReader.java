package com.esri.hadoop.quadtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Iterator;

/**
 * FSQuadTreeReader allows you to search over a stream, create one, search as many times
 * as you like and release when done.
 */
public class FSQuadTreeReader
{
    private final FSDataInputStream m_dataInputStream;
    private final QuadTree m_quadTree;

    /**
     * Create a FSQuadTreeReader
     *
     * @param dataInputStream the input data stream.
     */
    public FSQuadTreeReader(final FSDataInputStream dataInputStream) throws IOException
    {
        m_dataInputStream = dataInputStream;
        m_quadTree = new QuadTree(dataInputStream);
    }

    /**
     * Performs a extent search over the entire quad tree and then executes the method
     * evaluateFunction(PointData) on all data in the extent (contained or touch)
     */
    public void search(
            final Extent extent,
            final IEvaluateFunction evaluateFunction) throws IOException
    {
        m_quadTree.search(m_dataInputStream, extent, evaluateFunction);
    }

    public Iterator<PointData> search(final Extent extent) throws IOException
    {
        return m_quadTree.search(m_dataInputStream, extent);
    }

    /**
     * Depth first search
     *
     * @param nodeFunction
     * @throws java.io.IOException
     */
    public void depthFirstSearch(final INodeFunction nodeFunction) throws IOException
    {
        m_quadTree.depthSearchFirst(m_dataInputStream, nodeFunction);
    }

}
