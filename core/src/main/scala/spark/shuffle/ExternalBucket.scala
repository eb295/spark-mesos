package spark.shuffle

import spark.BufferCollection
import spark.SizeEstimator

/**
 * External hashing implementation.
 * Initialized in ShuffledRDD and ShuffleMapTask.
 * 
 * @constructor default five-parameter constructor is directly called to begin recursive hashing.
 * @param inMemBucket an InternalBucket that holds KVs added in memory (size > maxBytes)
 * @param numInserted the number of KVs (= number of put() or merge() calls)
 *        on the InternalBucket passed above.
 * @param avgObjSize the average size of each KV passed to a put() or merge() call.
 * @param numPartitions number of partition files to use for external hashing.
 * @param maxBytes maximum in-memory bytes to use for hashing
 */
class ExternalBucket[K, V, C](
    private val inMemBucket: InternalBucket[K, V, C],
    private var numInserted: Long,
    private var avgObjSize: Long,
    private val numPartitions: Int,
    private val maxBytes: Long)
  extends ShuffleBucket[K, V, C] {

  private val blockBufferSize = maxBytes/numPartitions
  private val fileBuffers = new BufferCollection[(K, C)](
    "externalBucket",
    numPartitions,
    blockBufferSize)
  private var bucketSize = 0L

  /** Write out entries passed from InMemBucket. */
  if (numInserted > 0) clearBucketToDisk()

  /** Constructor called in ShuffledRDD and ShuffleMapTask. */
  def this(
    inMemBucket: InternalBucket[K, V, C],
    numInserted: Long,
    avgObjSize: Long,
    maxBytes: Long) = {
    this(
      inMemBucket,
      numInserted,
      avgObjSize,
      ShuffleBucket.getNumPartitions,
      maxBytes)
  }

  def put(key: K, value: V) {
    inMemBucket.put(key, value)
    updateBucket()
  }

  def merge(key: K, newC: C) {
    inMemBucket.merge(key, newC)
    updateBucket()
  }

  /**
   * Write out entries in InMemBucket and update the avgObjSize.
   * Avg combiner size is tracked as fileBuffers.avgObjSize.
   */
  private def clearBucketToDisk() {
    val inMem = inMemBucket.bucketIterator().toArray
    val size = SizeEstimator.estimate(inMem)
    avgObjSize = (avgObjSize + maxBytes/numInserted)/2
    inMem.foreach(kc => writeToPartition(kc._1, kc._2))
    inMemBucket.clear()
    numInserted = 0L
    bucketSize = 0L
  }

  /** Called whenever an object is put/merged. Updates size of the inMemBucket. */
  private def updateBucket() {
    bucketSize += avgObjSize
    numInserted += 1
    if (bucketSize > maxBytes) clearBucketToDisk()
  }

  /** Write to a FileBuffer, using hashcode as index. */
  private def writeToPartition(key: K, toWrite: C) {
    var partitionFileNum = key.hashCode % numPartitions
    if (partitionFileNum < 0) {
      partitionFileNum = partitionFileNum + numPartitions
    }
    val tup = (key, toWrite)
    fileBuffers.write(tup, partitionFileNum)
  }

  def bucketIterator(): Iterator[(K, C)] = {
    /** Force all tuples in memory (in hashMap, buffers) to disk. */
    if (numInserted > 0) clearBucketToDisk()

    for (i <- 0 until fileBuffers.numFiles) { fileBuffers.forceToDisk(i) }

    /** Delete files that haven't been written to */
    fileBuffers.deleteEmptyFiles()

    return new Iterator[(K, C)] {
      var bucketToRead = 0
      val buckets = fileBuffers.numFiles

      /** Merge tuples in the next partition file using inMemBucket. */
      def loadNextPartition(): Iterator[(K, C)] = {
        inMemBucket.clear()
        val bufferIter = fileBuffers.getBufferedIterator(bucketToRead)
        val bucket = {
          if (fileBuffers.fitsInMemory(bucketToRead, maxBytes)) {
            inMemBucket
          } else {
            val partitions = numPartitions * 2
            new ExternalBucket(inMemBucket, 0, avgObjSize, partitions, maxBytes)
          }
        }
        while (bufferIter.hasNext) {
          val (k, c) = bufferIter.next()
          bucket.merge(k, c)
        }
        /** If we've read all buckets from disk, delete all files */
        bucketToRead += 1
        if (bucketToRead == buckets) fileBuffers.clear()
        bucket.bucketIterator
      }

      var bucketIter = loadNextPartition()

      def hasNext() = bucketIter.hasNext || bucketToRead < buckets

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
