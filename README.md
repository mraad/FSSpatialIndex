FSSpatialIndex
==============

Spatial Index on Hadoop File System

### Building and Installing

This project is built using [maven](http://maven.apache.org/)

```
$ mvn clean package
```

### Description
This project is based on the works of my friend and colleague Kerry C. that I fully trust when it comes to building spatial indexes (and other matters too :-)

There are two implementations in this package, a [QuadTree](http://en.wikipedia.org/wiki/Quadtree) and an [RTree](http://en.wikipedia.org/wiki/Rtree). Although a QuadTree can index any geometry with a bounding box rectangle, this implementation is very specific to 2D points only and is loosely based on HDFS [MapFiles](http://hadoop.apache.org/docs/r2.2.0/api/org/apache/hadoop/io/MapFile.html).  However, unlike MapFiles, the index file does _not_ have to be a sibling to the data file and is _not_ read into memory when a reference to it is instantiated. In addition, the data does _not_ have to be in HDFS, it could be in a in-memory database. When performing a search based on an ```Extent```, the return [iterator](http://docs.oracle.com/javase/7/docs/api/java/util/Iterator.html) intelligently caches the index buckets right of HDFS. The ```PointData``` has a property ```address``` that enables you to seek into the data. 

```
final FSDataInputStream dataInputStream = fileSystem.open(dataPath);
final FSDataInputStream indexInputStream = fileSystem.open(indexPath);
final FSQuadTreeReader reader = new FSQuadTreeReader(indexInputStream);

final Iterator iterator = reader.search(new Extent(...));
while (iterator.hasNext())
{
    final PointData pointData = iterator.next();
    dataInputStream.seekTo(pointData.address);
    ...
}
```

Building the index is a single process function. Again, though this is targeting a BigData project on Hadoop, the index building was not designed to use parallelism in this very specific project as the data is relatively "small". In addition, the raw data is already stored into HDFS and is later indexed. Will be very interesting to write a function that restore the data in a spatial index order rather than the default order (usually time based) to minimize sequential seek jumps.

Each record is tokenized, transformed into a ```PointData``` and inserted into the tree. Make sure to ```close``` the writer to flush the tree content onto HDFS.

```
final FSDataOutputStream dataOutputStream = fileSystem.create(pathIndex, true);
final FSQuadTreeWriter quadTreeWriter = new FSQuadTreeWriter(dataOutputStream, bucketSize, extent);
try
{
    // Open input
    try
    {
        while (input.hasData())
        {
            final PointData pointData = input.readPointData()
            quadTreeWriter.addPointData(pointData);
        }
    }
    finally
    {
       // Close input
    }
}
finally
{
    quadTreeWriter.close(); // YOU MUST CLOSE THE WRITER !!!
}
```
