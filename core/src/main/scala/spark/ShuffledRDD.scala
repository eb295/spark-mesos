package spark

import java.util.{Map => JMap, HashMap => JHashMap}

class ShuffledRDDSplit(val idx: Int) extends Split {
  override val index = idx
  override def hashCode(): Int = idx
}

class ShuffledRDD[K, V, C](
    @transient parent: RDD[(K, V)],
    aggregator: Aggregator[K, V, C],
    part : Partitioner) 
  extends RDD[(K, C)](parent.context) {

  override val partitioner = Some(part)
  
  @transient
  val splits_ = Array.tabulate[Split](part.numPartitions)(i => new ShuffledRDDSplit(i))

  override def splits = splits_
  
  override def preferredLocations(split: Split) = Nil
  
  val makeMap: () => JMap[Any, Any] = ShuffleBucket.makeMap[K, C]
  val dep = new ShuffleDependency(context.newShuffleId, parent, aggregator, part, makeMap)
  override val dependencies = List(dep)

  override def compute(split: Split): Iterator[(K, C)] = {
    val maxBytes = ShuffleBucket.getMaxHashBytes
    var combiners: ShuffleBucket[K, V, C] = new InternalBucket(aggregator, makeMap(), maxBytes)
    var bytesUsed = 0L
    var pairsMerged = 0
    var avgCombinerSize = 0L
    var usingExternalHash = false

    def mergePair(k: K, c: C) {
      combiners.merge(k, c)
      pairsMerged += 1
      if (pairsMerged == 1000) {
        bytesUsed = SizeEstimator.estimate(combiners)
        avgCombinerSize = bytesUsed/1000
      }
      if (!usingExternalHash && bytesUsed > maxBytes) {
        combiners = 
          new ExternalBucket(combiners.asInstanceOf[InternalBucket[K, V, C]], pairsMerged, avgCombinerSize)
          usingExternalHash = true
      } else {
        bytesUsed += avgCombinerSize
      }
    }

    val fetcher = SparkEnv.get.shuffleFetcher
    fetcher.fetch[K, C](dep.shuffleId, split.index, mergePair)
    return combiners.bucketIterator()
  }
}
