package spark;

import java.util.{HashMap => JHashMap, Map => JMap}
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream
import it.unimi.dsi.fastutil.objects.{Object2BooleanOpenHashMap, Object2ByteOpenHashMap,
				      Object2CharOpenHashMap, Object2DoubleOpenHashMap, 
				      Object2FloatOpenHashMap, Object2IntOpenHashMap, 
				      Object2LongOpenHashMap, Object2ObjectOpenHashMap, 
				      Object2ShortOpenHashMap}


class ExternalBucket[K, V, C](
    val inMemBucket: InternalBucket[K, V, C], 
    var numInserted: Int,
    var avgObjSize: Long)
  extends ShuffleBucket[K, V, C] {

  private val maxBytes = inMemBucket.maxBytes
  private val numPartitions = ShuffleBucket.getNumPartitions
  private val bufferSize = maxBytes/numPartitions
  // Contains (K, V) pairs for map-side, or (K, C) pairs for reduce-side
  private val fileBuffers = new BufferCollection[(K, Any)]("externalHash", numPartitions, bufferSize)
  private var bucketSize = 0L

  // Write out entries passed in InMemBucket
  clearBucketToDisk()

  // Write out entries in InMemBucket
  private def clearBucketToDisk() = {
    val inMem = inMemBucket.bucketIterator().toArray
    val size = SizeEstimator.estimate(inMem)
    val avgCombinerSize = size/inMemBucket.hashMap.size
    avgObjSize = (avgObjSize + size/numInserted)/2
    inMem.foreach(kc => writeToPartition(kc._1, kc._2, avgCombinerSize))
    inMemBucket.clear()
    numInserted = 0
    bucketSize = 0L
  }

  // Update counts whenever an object is put/merged
  private def updateBucket() = {
    bucketSize += avgObjSize
    numInserted += 1
    if (bucketSize > maxBytes) clearBucketToDisk()
  }
  
  def put(key: K, value: V) = {
    inMemBucket.put(key, value)
    updateBucket()
  }
  
  def merge(key: K, newC: C) = {
    inMemBucket.merge(key, newC)
    updateBucket()
  }
  
  private def writeToPartition(key: K, toWrite: C, size: Long) = {
    var partitionFileNum = key.hashCode % numPartitions
    // Handle negative hashcodes
    if (partitionFileNum < 0) {
      partitionFileNum = partitionFileNum + numPartitions
    }
    val tup = (key, toWrite)
    fileBuffers.write(tup, partitionFileNum, size)
  }

  def bucketIterator(): Iterator[(K, C)] = {
    // Write out InMemBucket
    if (numInserted > 0) clearBucketToDisk()
    
    // Force all buffers to disk.
    for (i <- 0 until fileBuffers.numBuffers) fileBuffers.forceToDisk(i)

    // Delete files that haven't been written to
    fileBuffers.deleteEmptyFiles()

    return new Iterator[(K, C)] {
      var buffersRead = 0
      val buffersToRead = fileBuffers.numBuffers

      // Load the next partition into the in-memory hash table, and return its iterator.
      def loadNextPartition(): Iterator[(K, C)] = {
        val bufferIter = fileBuffers.getBufferedIterator(0).asInstanceOf[Iterator[(K, C)]]
        val merger = {
          if (fileBuffers.fitsInMemory(0, maxBytes)) {
            inMemBucket
          } else {
            new ExternalBucket(inMemBucket, 0, fileBuffers.avgObjSize)
          }
        }
        while (bufferIter.hasNext) {
          val (k, c) = bufferIter.next()
          merger.merge(k, c)
        }
        buffersRead += 1
        // Delete file once we've iterated through its contents
        fileBuffers.delete(0)
        merger.bucketIterator
      }
      
      // Load the first partition into in-memory hash table
      var bucketIter = loadNextPartition()
      
      def hasNext() = bucketIter.hasNext || buffersRead < buffersToRead
      
      // Returns an iterator over all the elements we want to hash
      def next(): (K, C) = {
        if (!bucketIter.hasNext) {
          inMemBucket.clear()
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


class InternalBucket[K, V, C](
    val aggregator: Aggregator[K, V, C],
    val hashMap: JMap[Any, Any],
    val maxBytes: Long)
  extends ShuffleBucket[K, V, C] {
  
  def put(key: K, value: V) = {
    val existing = hashMap.get(key)
    if (existing == null) {
      hashMap.put(key, aggregator.createCombiner(value))
    } else {
      hashMap.put(key, aggregator.mergeValue(existing.asInstanceOf[C], value))
    }
  }

  def merge(key: K, c: C) {
    val existing = hashMap.get(key) 
    if (existing == null) {
      hashMap.put(key, c)
    } else {
      hashMap.put(key, aggregator.mergeCombiners(c, existing.asInstanceOf[C]))
    }
  }
  
  def clear() = hashMap.clear()

  def bucketIterator(): Iterator[(K, C)] = {
    return new Iterator[(K, C)] {
      val iter = hashMap.entrySet.iterator

      def hasNext() = iter.hasNext

      def next() = {
        val entry = iter.next()
        (entry.getKey.asInstanceOf[K], entry.getValue.asInstanceOf[C])
      }
    }
  }
}

trait ShuffleBucket[K, V, C] {
  def put(key: K, value: V)
  def merge(key: K, value: C)
  def clear()
  def bucketIterator(): Iterator[(K, C)]
}

object ShuffleBucket {
  // Must cast to JMap[K, C], or else it won't compile
  implicit def createObject2BooleanMap[K <: AnyRef, C <: Boolean] =
    () => new Object2BooleanOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2ByteMap[K <: AnyRef, C <: Byte] =
    () => new Object2ByteOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2CharMap[K <: AnyRef, C <: Byte] =
    () => new Object2CharOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2IntMap[K <: AnyRef, C <: Int] = 
    () => new Object2IntOpenHashMap[K].asInstanceOf[JMap[K, Int]]

  implicit def createObject2IntegerMap[K <: AnyRef, C <: Integer] = 
    () => new Object2IntOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2DoubleMap[K <: AnyRef, C <: Double] =
    () => new Object2DoubleOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2FloatMap[K <: AnyRef, C <: Float] = 
    () => new Object2FloatOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2LongMap[K <: AnyRef, C <: Long] = 
    () => new Object2LongOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2ShortMap[K <: AnyRef, C <: Short] = 
    () => new Object2ShortOpenHashMap[K].asInstanceOf[JMap[K, C]]

  implicit def createObject2ObjectMap[K <: AnyRef, C <: AnyRef] = 
    () => new Object2ObjectOpenHashMap[K, C].asInstanceOf[JMap[K, C]]
    
  // Cast to JMap[Any, Any] to get put() to work.
  def makeMap[K, C] (implicit m: () => JMap[K, C] = () => new JHashMap[K, C]) = {
    m.asInstanceOf[() => JMap[Any, Any]]
  }
    
  def getNumPartitions = System.getProperty("spark.outOfCoreUtils.numParitionFiles", "64").toInt

  def getMaxHashBytes(): Long = {
    val hashMemFractToUse = System.getProperty("spark.shuffledRDD.hashFraction", "0.25").toDouble
    (Runtime.getRuntime.maxMemory * hashMemFractToUse).toLong
  }
}
