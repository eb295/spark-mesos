package spark.shuffle

import java.util.{Map => JMap, HashMap => JHashMap}

import spark._

class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K: ClassManifest, V, C: ClassManifest](
    @transient parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner) 
  extends RDD[(K, C)](parent.context) {

  override val partitioner = Some(part)
  
  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  val kClass = implicitly[ClassManifest[K]].erasure.asInstanceOf[Class[K]]
  val cClass = implicitly[ClassManifest[C]].erasure.asInstanceOf[Class[C]]
  val createMap: () => JMap[K, C] = ShuffleBucket.makeMap(kClass, cClass)
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part, createMap)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val maxBytes = ShuffleBucket.getMaxHashBytes
    var combiners: ShuffleBucket[K, V, C] = new InternalBucket(aggregator, createMap())
    var bytesUsed = 0L
    var pairsMerged = 0L
    var avgPairSize = 0L
    var usingExternalHash = false

    def mergePair(k: K, c: C) {
      combiners.merge(k, c)
      pairsMerged += 1L
      if (!usingExternalHash) {
        if (pairsMerged % 100000 == 0) {
          bytesUsed = SizeEstimator.estimate(combiners)
          avgPairSize = bytesUsed/pairsMerged
        }
        if (bytesUsed > maxBytes) {
          bytesUsed = SizeEstimator.estimate(combiners)
          /* Check to make sure we've gone over */
          if (bytesUsed > maxBytes) {
            val newBucket = new ExternalBucket(
              combiners.asInstanceOf[InternalBucket[K, V, C]], 
              pairsMerged, 
              avgPairSize, 
              maxBytes)
              usingExternalHash == true
            combiners = newBucket
          } else {
            avgPairSize = bytesUsed/pairsMerged
          }
        } else {
          bytesUsed += avgPairSize
        }
      }
    }

    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[K, C](dep.shuffleId, split.index, mergePair)
    return combiners.bucketIterator()
  }
}
