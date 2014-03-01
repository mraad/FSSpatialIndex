package com.esri.hadoop.quadtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.awt.geom.Point2D;
import java.io.IOException;

/**
 * FSQuadTreeWriter allows you to write a quad tree to a stream, thus enabling you to spatially index.
 * Create one, add multiple points and then close.
 */
public class FSQuadTreeWriter
{
    public static final int START_LEVEL = 16;
    public static final int MINIMUM_LEVEL = 0;
    public static final int MAXIMUM_LEVEL = 25;

    private final QuadTree m_quadTree;
    private final FSDataOutputStream m_stream;

    /**
     * Create a FSQuadTreeWriter
     *
     * @param stream     the output stream
     * @param bucketSize the bucket size (suggested value 32 for now)
     * @param fullExtent the best extent you can figure out (it will automatically widen)
     */
    public FSQuadTreeWriter(
            final FSDataOutputStream stream,
            final int bucketSize,
            final Extent fullExtent)
    {
        m_stream = stream;
        m_quadTree = new QuadTree(bucketSize, START_LEVEL, MINIMUM_LEVEL, MAXIMUM_LEVEL,
                fullExtent.width(), new Point2D.Double(fullExtent.xmin, fullExtent.ymin));
    }

    /**
     * Add a point
     *
     * @param pointData
     */
    public void addPointData(final PointData pointData)
    {
        m_quadTree.addPointData(pointData);
    }

    /**
     * Done adding points; write then, shut down and release
     */
    public void close() throws IOException
    {
        try
        {
            m_quadTree.write(m_stream);
        }
        finally
        {
            m_quadTree.release();
            m_stream.close();
        }
    }
}
