package spark.shuffle

import spark.BufferCollection
import spark.SizeEstimator

class ExternalBucket[K, V, C](
    private val inMemBucket: InternalBucket[K, V, C], 
    private var numInserted: Int,
    private var avgObjSize: Long,
    private val numPartitions: Int,
    private val avgCombinerSize: Long)
  extends ShuffleBucket[K, V, C] {

  private val maxBytes = inMemBucket.maxBytes
  private val bufferSize = maxBytes/numPartitions
  // Pass avg size of combiner, used for managing buffer memory size.
  private val fileBuffers = new BufferCollection[(K, Any)](
    "externalBucket", 
    numPartitions, 
    bufferSize, 
    avgCombinerSize)
  private var bucketSize = 0L
  // Write out entries passed from InMemBucket.
  if (numInserted > 0) clearBucketToDisk()

  def this(inMemBucket: InternalBucket[K, V, C], numInserted: Int, avgObjSize: Long) = {
    this(
      inMemBucket, 
      numInserted, 
      avgObjSize, 
      ShuffleBucket.getNumPartitions, 
      inMemBucket.maxBytes/inMemBucket.numCombiners)
  }

  def put(key: K, value: V) {
    inMemBucket.put(key, value)
    updateBucket()
  }
  
  def merge(key: K, newC: C) {
    inMemBucket.merge(key, newC)
    updateBucket()
  }
  
  // Write out entries in InMemBucket and update the avgObjSize.
  // Avg combiner size is tracked as fileBuffers.avgObjSize
  private def clearBucketToDisk() {
    val inMem = inMemBucket.bucketIterator().toArray
    val size = SizeEstimator.estimate(inMem)
    avgObjSize = (avgObjSize + maxBytes/numInserted)/2
    inMem.foreach(kc => writeToPartition(kc._1, kc._2))
    inMemBucket.clear()
    numInserted = 0
    bucketSize = 0L
  }

  // Called whenever an object is put/merged
  private def updateBucket() {
    bucketSize += avgObjSize
    numInserted += 1
    if (bucketSize > maxBytes) clearBucketToDisk()
  }

  private def writeToPartition(key: K, toWrite: C) {
    var partitionFileNum = key.hashCode % numPartitions
    // Handle negative hashcodes
    if (partitionFileNum < 0) {
      partitionFileNum = partitionFileNum + numPartitions
    }
    val tup = (key, toWrite)
    fileBuffers.write(tup, partitionFileNum)
  }

  def bucketIterator(): Iterator[(K, C)] = {
    // Write out InMemBucket
    if (numInserted > 0) clearBucketToDisk()
    
    // Force all buffers to disk.
    for (i <- 0 until fileBuffers.numBuffers) { fileBuffers.forceToDisk(i) }

    // Delete files that haven't been written to
    fileBuffers.deleteEmptyFiles()

    return new Iterator[(K, C)] {
      var buffersRead = 0
      val buffersToRead = fileBuffers.numBuffers

      // Load the next partition into the in-memory hash table, and return its iterator.
      def loadNextPartition(): Iterator[(K, C)] = {
        inMemBucket.clear()
        val bufferIter = fileBuffers.getBufferedIterator(0).asInstanceOf[Iterator[(K, C)]]
        val bucket = {
          if (fileBuffers.fitsInMemory(0, maxBytes)) {
            inMemBucket
          } else {
            val partitions = (numPartitions * 1.5).toInt
            new ExternalBucket(inMemBucket, 0, avgObjSize, partitions, fileBuffers.avgObjSize)
          }
        }
        while (bufferIter.hasNext) {
          val (k, c) = bufferIter.next()
          bucket.merge(k, c)
        }
        buffersRead += 1
        // Delete file once we've iterated through its contents
        fileBuffers.delete(0)
        bucket.bucketIterator
      }
      
      // Load the first partition into in-memory hash table
      var bucketIter = loadNextPartition()
      
      def hasNext() = bucketIter.hasNext || buffersRead < buffersToRead
      
      // Returns an iterator over all the elements we want to hash
      def next(): (K, C) = {
        if (!bucketIter.hasNext) {
          bucketIter = loadNextPartition()
        }
        val nextKV = bucketIter.next()
        return nextKV
      }

      def toArray = bucketIter.toArray
    }
  }

  def clear() = {
    inMemBucket.clear()
  }
}
