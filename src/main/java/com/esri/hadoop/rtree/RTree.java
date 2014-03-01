package com.esri.hadoop.rtree;

import com.esri.hadoop.Extent;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * The ubiquitous RTree
 * per Anthony Gutman's paper
 */

public class RTree
{
    private Node m_root;        //the root node
    private int m_nodeLowSize; //the minimum size of a node
    private int m_nodeHighSize;//the maximum size of a node
    private long m_rootHandle;
    private FSDataInputStream m_stream;

    /**
     * Construct an RTree using the given object database as storage
     *
     * @param nodeLowSize
     * @param nodeHighSize
     */
    public RTree(
            final int nodeLowSize,
            final int nodeHighSize)
    {
        m_nodeLowSize = nodeLowSize;
        m_nodeHighSize = nodeHighSize;
        m_root = new LeafNode();
    }

    /**
     * Constructs the RTree using a stream.
     * (It only reads the node sizes, no it does not instantiate the entire tree)
     *
     * @param is the input stream
     * @throws IOException
     */
    public RTree(FSDataInputStream is) throws IOException
    {
        m_stream = is;
        read(is);
    }

    /**
     * Release the rtree
     */
    public void release()
    {
        m_root.release(true);
    }

    /**
     * Print the tree
     */
    void print()
    {

        m_root.print();
    }

    /**
     * Searches R tree by using the iterator
     *
     * @param extent the extent
     * @return the iterator.
     * @throws IOException
     */
    public Iterator<MBRHandle> search(Extent extent) throws IOException
    {
        RTreeIterator iterator = new RTreeIterator(this, m_stream, extent);

        iterator.addNodeHandle(m_rootHandle);
        return iterator;
    }

    /**
     * Reads the extent from the input stream
     *
     * @param is the input
     * @return the extent read
     * @throws IOException
     */
    private Extent readExtent(FSDataInputStream is) throws IOException
    {
        final double xmin = is.readDouble();
        final double ymin = is.readDouble();
        final double xmax = is.readDouble();
        final double ymax = is.readDouble();
        return new Extent(xmin, ymin, xmax, ymax);
    }

    /**
     * Searches a node by reading the node using a handle, which ultimately loads up the iterator.
     * This loads the iterator as it goes, not all at once
     *
     * @param iterator the iterator
     * @param stream   the input stream
     * @param extent   the extent searching over
     * @param handle   the handle to use to read the node from.
     * @throws IOException
     */
    public void searchNode(
            final RTreeIterator iterator,
            final FSDataInputStream stream,
            final Extent extent,
            final long handle) throws IOException
    {
        stream.seek(handle);
        final boolean isLeaf = stream.readBoolean();
        final int size = stream.readInt();
        for (int i = 0; i < size; i++)
        {
            final long dataHandle = stream.readLong();
            final Extent dataExtent = readExtent(stream);
            if (!dataExtent.isDisjoint(extent))
            {
                if (isLeaf)
                {
                    iterator.addMBRHandle(new MBRHandle(dataExtent, dataHandle));
                }
                else
                {
                    iterator.addNodeHandle(dataHandle);
                }
            }
        }
    }

    /**
     * Retrieves a node's minimum size
     *
     * @return the minimum size of a node
     */
    public int getNodeMinimumSize()
    {
        return this.m_nodeLowSize;
    }

    /**
     * Retrieves a node's maximum size
     *
     * @return the maximum size of a node
     */
    public int getNodeMaximumSize()
    {
        return this.m_nodeHighSize;
    }

    /**
     * Inserts a data element
     *
     * @param data
     */
    public void insert(MBRHandle data)
    {

        Node node = m_root;
        Node rNode = node.insert(data);
        if (rNode != null)
        {
            //root Node was split
            Node newRoot = new InnerNode();

            newRoot.insertNonFull(node);
            newRoot.insertNonFull(rNode);

            m_root = newRoot;
        }
    }

    /**
     * Writes to stream
     *
     * @param os
     * @throws java.io.IOException
     */
    public void write(final FSDataOutputStream os) throws IOException
    {
        os.writeInt(m_nodeLowSize);
        os.writeInt(m_nodeHighSize);

        m_root.calculateHandles(os.getPos());
        m_root.write(os);
    }

    public void read(final FSDataInputStream os) throws IOException
    {
        m_nodeLowSize = os.readInt();
        m_nodeHighSize = os.readInt();
        m_rootHandle = os.getPos();
    }

    /**
     * A leaf node contains other nodes and has NO MBR Handles
     */
    public class LeafNode extends Node
    {

        /**
         * Default ctor
         */
        public LeafNode()
        {
        }

        /**
         * Indicates that this node is a leaf node
         *
         * @return
         */
        public boolean isLeafNode()
        {
            return true;
        }

        /**
         * Creates a node which is similar to this one.
         *
         * @return the leaf node
         */
        public Node createNode()
        {
            return new LeafNode();
        }

        /**
         * Prints out this node
         */
        public void print()
        {
            System.out.println("LeafNode " + m_extent.toString());
            for (Object objNode : m_data)
            {
                MBRHandle node = (MBRHandle) objNode;
                System.out.println("Leaf handle:" + node.handle + "  " + node.getExtent().toString());
            }
        }

        /**
         * Inserts a MBRHandle
         *
         * @param data the MBR and handle
         * @return
         */
        public Node insert(MBRHandle data)
        {
            if (m_data.size() >= RTree.this.getNodeMaximumSize())
            {
                Node node = splitAndInsert(data);
                return node;
            }
            else
            {
                insertNonFull(data);
                return null;
            }
        }
    }

    /**
     * The inner node (There's leaf nodes and inner nodes)
     */
    public class InnerNode extends Node
    {
        /**
         * Default ctor
         */
        public InnerNode()
        {

        }

        /**
         * Indicates that this is not a leaf node
         *
         * @return false
         */
        public boolean isLeafNode()
        {
            return false;
        }

        /**
         * Creates a node similar to this one (an inner node)
         *
         * @return the inner node created
         */
        public Node createNode()
        {
            return new InnerNode();
        }

        /**
         * Prints out this node
         */
        public void print()
        {
            System.out.println("Node " + m_extent.toString());
            for (Object objNode : m_data)
            {
                Node node = (Node) objNode;
                System.out.println("Pointer " + node.getExtent().toString());
            }
            for (Object objNode : m_data)
            {
                Node node = (Node) objNode;
                node.print();
            }
        }

        /**
         * Inserts an MBR and handle (not really)
         * It will ultimately be inserted on a leaf node.
         *
         * @param data the MBR and handle
         * @return
         */
        public Node insert(MBRHandle data)
        {

            double area, darea, minarea = 1e60, iarea = 1e60, increase;
            Node inode = null;
            if (m_data.size() == 0)
            {
                return null;
            }

            Extent dataExtent = data.extent;
            Extent saveExtent = new Extent();
            for (Object objNode : m_data)
            {
                saveExtent.set(dataExtent);
                Node nodeData = (Node) objNode;
                Extent nodeExtent = nodeData.getExtent();

                saveExtent.union(nodeExtent);

                area = nodeExtent.area();
                darea = saveExtent.area();
                increase = darea - area;
                if (increase < minarea)
                {
                    inode = nodeData;
                    minarea = area;
                }
                else if (increase == minarea)
                {
                    if (area < minarea)
                    {
                        inode = nodeData;
                        minarea = area;
                    }
                }
            }

            //TODO:what if inode is null
            if (inode == null)
            {
                System.out.println("Another problem");
            }
            Node rNode = inode.insert(data);
            if (rNode != null)
            {
                Node sNode = insert1(rNode);
                return sNode;
            }
            else
            {
                widen(inode);
            }

            return null;
        }
    }

    /**
     * A node (either inner or leaf)
     */
    abstract public class Node extends RTreeData
    {
        protected ArrayList m_data;
        protected long m_handle;

        protected Extent m_extent = Extent.NULL_EXTENT.clone();

        /**
         * Default ctor
         */
        public Node()
        {
            m_data = new ArrayList();
        }

        /**
         * Indicates if this is a leaf node
         *
         * @return true for a leaf node else false
         */
        abstract public boolean isLeafNode();

        /**
         * Prints out this node
         */
        abstract public void print();

        /**
         * Inserts an MBR and handle
         *
         * @param data the MBR and handle
         * @return the node if one was created
         */
        abstract public Node insert(MBRHandle data);

        /**
         * Retrieves the handle of this node
         *
         * @return
         */
        public long getHandle()
        {
            return m_handle;
        }

        /**
         * Writes the extent for this node
         *
         * @param os     the output stream
         * @param extent the extent
         * @throws IOException
         */
        protected void writeExtent(
                final FSDataOutputStream os,
                final Extent extent) throws IOException
        {
            os.writeDouble(extent.xmin);
            os.writeDouble(extent.ymin);
            os.writeDouble(extent.xmax);
            os.writeDouble(extent.ymax);
        }

        /**
         * Writes out this node
         *
         * @param os
         * @throws java.io.IOException
         */
        public void write(FSDataOutputStream os) throws IOException
        {
            final Extent extent = Extent.NULL_EXTENT.clone();

            os.writeBoolean(isLeafNode());
            os.writeInt(m_data.size());
            for (Object objData : m_data)
            {
                final RTreeData rTreeData = (RTreeData) objData;
                extent.unionInPlace(rTreeData.getExtent());
                os.writeLong(rTreeData.getHandle());
                writeExtent(os, rTreeData.getExtent());
            }
            if (!isLeafNode())
            {
                for (final Object objData : m_data)
                {
                    final Node node = (Node) objData;
                    node.write(os);
                }
            }
            if (!m_extent.isEqual(extent, 1e-6))
            {
                System.out.println("Data is not correct");
            }
        }

        /**
         * Releases this node
         *
         * @param isRoot true is this node is the root
         */
        public void release(boolean isRoot)
        {
            if (!isLeafNode())
            {
                for (Object objData : m_data)
                {
                    Node node = (Node) objData;
                    node.release(false);
                }
            }
        }

        /**
         * Calculates all the handles from this node and beneath
         *
         * @param startHandle the starting handle
         * @return the handle which would result if we wrote out everything
         */
        public long calculateHandles(long startHandle)
        {
            m_handle = startHandle;
            long newHandle = 40 * m_data.size() + 5 + startHandle;
            if (!isLeafNode())
            {
                for (Object objData : m_data)
                {
                    Node node = (Node) objData;
                    newHandle = node.calculateHandles(newHandle);
                }
            }
            return newHandle;
        }

        /**
         * Retrieves the extent for this node
         *
         * @return
         */
        public Extent getExtent()
        {
            return m_extent;
        }

        /**
         * Inserts a node into the tree
         *
         * @param node the node
         * @return
         */
        protected Node insert1(Node node)
        {
            if (m_data.size() >= RTree.this.getNodeMaximumSize())
            {
                Node newNode = splitAndInsert(node);
                return newNode;
            }
            else
            {
                insertNonFull(node);
                return null;
            }
        }

        /**
         * Widens the extent for this node
         *
         * @param node
         */
        protected void widen(Node node)
        {
            m_extent.unionInPlace(node.m_extent);
        }

        /**
         * Inserts data into the node (the node is not full)
         *
         * @param data
         */
        public void insertNonFull(RTreeData data)
        {
            m_data.add(data);
            m_extent.unionInPlace(data.getExtent());
        }

        /**
         * Re-calculates the area due to all objects inside the node
         */
        public void recalcArea()
        {
            m_extent.set(Extent.NULL_EXTENT);

            for (Object obj : m_data)
            {
                RTreeData data = (RTreeData) obj;
                m_extent.unionInPlace(data.getExtent());
            }
        }

        /**
         * Creates a node which is similar to this one.
         *
         * @return node
         */
        abstract public Node createNode();

        /**
         * Splits the node and then inserts the data.
         * See Algorithm Quadratic Split from Guttman's paper.
         */
        public Node splitAndInsert(RTreeData passedData)
        {
            double iarea, jarea, darea;
            double d, maxD = -1e60;
            int iPick = 0, jPick = 1;
            Extent dEnv = new Extent();
            ArrayList oldData = m_data;
            m_data = new ArrayList();
            m_extent.set(Extent.NULL_EXTENT);
            //PICK SEEDS
            for (int i = 0; i < oldData.size(); i++)
            {
                Extent iExtent = ((RTreeData) oldData.get(i)).getExtent();
                for (int j = 0; j < oldData.size(); j++)
                {
                    if (j != i)
                    {
                        Extent jExtent = ((RTreeData) oldData.get(j)).getExtent();
                        dEnv.set(iExtent);
                        dEnv.unionInPlace(jExtent);
                        iarea = iExtent.area();
                        jarea = jExtent.area();
                        darea = dEnv.area();
                        d = darea - iarea - jarea;
                        if (d > maxD)
                        {
                            iPick = i;
                            jPick = j;
                            maxD = d;
                        }
                    }
                }
            }

            //PICK NEXT
            Extent iEnv = ((RTreeData) oldData.get(iPick)).getExtent().clone();
            Extent jEnv = ((RTreeData) oldData.get(jPick)).getExtent().clone();
            Extent jTempEnv = new Extent();
            Extent iTempEnv = new Extent();
            Node iNode = this;
            Node jNode = createNode();
            iNode.insertNonFull((RTreeData) oldData.get(iPick));
            jNode.insertNonFull((RTreeData) oldData.get(jPick));
            double d1, d2, dd, maxDD;
            boolean marked[] = new boolean[oldData.size()];
            for (int i = 0; i < oldData.size(); i++)
            {
                marked[i] = true;
            }
            marked[iPick] = false;
            marked[jPick] = false;
            int jMax = -1;
            for (int i = 0; i < oldData.size(); i++)
            {
                maxDD = -1e60;
                for (int j = 0; j < oldData.size(); j++)
                {
                    if (marked[j])
                    {
                        iTempEnv.set(((RTreeData) oldData.get(j)).getExtent());
                        iTempEnv.unionInPlace(iEnv);
                        jTempEnv.set(((RTreeData) oldData.get(j)).getExtent());
                        jTempEnv.unionInPlace(jEnv);

                        d1 = iEnv.area() - iTempEnv.area();
                        d2 = jEnv.area() - jTempEnv.area();

                        dd = Math.abs(d1 - d2);
                        if (maxDD < dd)
                        {
                            maxDD = dd;
                            jMax = j;
                        }
                    }
                }
                if (jMax != -1)
                {
                    iTempEnv.set(((RTreeData) oldData.get(jMax)).getExtent());
                    iTempEnv.unionInPlace(iEnv);
                    jTempEnv.set(((RTreeData) oldData.get(jMax)).getExtent());
                    jTempEnv.unionInPlace(jEnv);

                    d1 = iEnv.area() - iTempEnv.area();
                    d2 = jEnv.area() - jTempEnv.area();
                    if (Math.abs(d1) < Math.abs(d2))
                    {
                        iNode.insertNonFull((RTreeData) oldData.get(jMax));
                        iEnv.set(iTempEnv);
                    }
                    else
                    {
                        jNode.insertNonFull((RTreeData) oldData.get(jMax));
                        jEnv.set(jTempEnv);
                    }
                    marked[jMax] = false;
                }
                else
                {
                    //System.out.println("jMax = -1");
                }
                jMax = -1;
            }
            for (boolean mark : marked)
            {
                if (mark)
                {
                    System.out.println("Problem");
                }
            }
            iTempEnv.set(passedData.getExtent());
            iTempEnv.unionInPlace(iEnv);
            iTempEnv.set(passedData.getExtent());
            iTempEnv.unionInPlace(jEnv);

            d1 = iEnv.area() - iTempEnv.area();
            d2 = jEnv.area() - jTempEnv.area();
            if (Math.abs(d1) < Math.abs(d2))
            {
                iNode.insertNonFull(passedData);
            }
            else
            {
                jNode.insertNonFull(passedData);
            }
            return jNode;
        }
    }

}

