package spark

import java.util.Random

class SampledRDDSplit(val prev: Split, val seed: Int) extends Split with Serializable {
  override val index: Int = prev.index
}

class SampledRDD[T: ClassManifest](
    prev: RDD[T],
    withReplacement: Boolean, 
    frac: Double,
    seed: Int)
  extends RDD[T](prev.context) {

  @transient
  val splits_ = {
    val rg = new Random(seed)
    prev.splits.map(x => new SampledRDDSplit(x, rg.nextInt))
  }

  override def splits = splits_.asInstanceOf[Array[Split]]

  override val dependencies = List(new OneToOneDependency(prev))
  
  override def preferredLocations(split: Split) =
    prev.preferredLocations(split.asInstanceOf[SampledRDDSplit].prev)

  override def compute(splitIn: Split) = {
    val split = splitIn.asInstanceOf[SampledRDDSplit]
    val rg = new Random(split.seed)
    if (withReplacement) {
      // Reservoir Sampling.
      // TODO: Avoid the extra pass through partition data that's needed to
      // determine the number of elements in this partition.
      var dataSize = 0L
      val countIter = prev.iterator(split.prev)
      while (countIter.hasNext) { dataSize += 1L }
      val dataIter = prev.iterator(split.prev)
      val sampleSize = (dataSize * frac).ceil.toInt
      val sampleReservoir = new Array[T](sampleSize)
      var indexToReplace = 0
      var i = 0
      while (dataIter.hasNext) {
        if (i < dataSize) {
          // Init reservoir, load sampleSize elements into memory.
          sampleReservoir(i) = dataIter.next()
        } else {
          // Keep the ith element with probability dataSize/(dataSize + i).
          indexToReplace = rg.nextInt(i)
          if (indexToReplace < sampleSize) {
            // If we keep i, replace a random element in the reservoir.
            sampleReservoir(indexToReplace) = dataIter.next()
          } else {
            dataIter.next()
          }
        }
        i += 1
      }
      sampleReservoir.iterator
    } else { // Sampling without replacement

      prev.iterator(split.prev).filter(x => (rg.nextDouble <= frac))
    }
  }
}
