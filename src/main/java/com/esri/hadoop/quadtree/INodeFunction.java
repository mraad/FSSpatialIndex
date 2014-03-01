package com.esri.hadoop.quadtree;

/**
 * This function interface is used by the DFS method, allowing you to perform
 * a depth first search over the tree...Do this with the reader (FSQuadTreeReader).
 */
public interface INodeFunction
{
    /**
     * This is the function you implement.
     *
     * @param node       the node
     * @param lowerLeftX the lower left lowerLeftX value
     * @param lowerLeftY the lower left lowerLeftY value
     * @param nodeWidth  the nodeWidth of the node
     * @param level      the level in the tree
     */
    public void evaluate(
            final QuadTreeNode node,
            final double lowerLeftX,
            final double lowerLeftY,
            final double nodeWidth,
            final int level);
}
