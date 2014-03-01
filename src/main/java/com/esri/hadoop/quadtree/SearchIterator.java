package com.esri.hadoop.quadtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataInputStream;

import java.util.Iterator;
import java.util.Stack;

/**
 * SearchIterator allows one to search
 */
public class SearchIterator implements Iterator<PointData>
{
    private Stack<SearchNode> m_nodes;
    private Stack<PointData> m_points;
    private Extent m_extent;
    private FSDataInputStream m_dataInputStream;

    /**
     * Construct a search iterator
     *
     * @param dataInputStream the input dataInputStream
     * @param extent          the extent being searched
     */
    public SearchIterator(
            final FSDataInputStream dataInputStream,
            final Extent extent)
    {
        m_dataInputStream = dataInputStream;
        m_extent = extent;
        m_nodes = new Stack<SearchNode>();
        m_points = new Stack<PointData>();
    }

    @Override
    public boolean hasNext()
    {
        if (!m_points.isEmpty())
        {
            return true;
        }
        if (m_nodes.isEmpty())
        {
            return false;
        }
        final SearchNode searchNode = m_nodes.pop();
        try
        {
            searchNode.node.search(this, m_extent, m_dataInputStream, searchNode.x, searchNode.y, searchNode.width);
        }
        finally
        {
            searchNode.node.clearThisNode();
            return hasNext();
        }
    }

    @Override
    public PointData next()
    {
        return m_points.pop();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("remove");
    }

    public void addPoint(PointData point)
    {
        m_points.push(point);
    }

    /**
     * Add a node to the iterator
     *
     * @param node  the quad tree node
     * @param x     the calling parameter x for searching
     * @param y     the calling parameter y for searching
     * @param width the calling parameter width for searching
     */
    public void addChild(
            final QuadTreeNode node,
            final double x,
            final double y,
            final double width)
    {
        m_nodes.push(new SearchNode(node, x, y, width));
    }

    /**
     * Simple class to hold the calling parameters for the node
     */
    private final class SearchNode
    {
        private QuadTreeNode node;
        private double x;
        private double y;
        private double width;

        /**
         * Simple ctor for this simple class
         *
         * @param node  a quad tree node
         * @param x     the x param
         * @param y     the y param
         * @param width the width param...all these params are called in the search of the node
         */
        public SearchNode(
                final QuadTreeNode node,
                final double x,
                final double y,
                final double width)
        {
            this.node = node;
            this.x = x;
            this.y = y;
            this.width = width;
        }
    }
}
