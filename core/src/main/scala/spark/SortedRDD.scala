package spark

import scala.collection.mutable.ArrayBuffer

class SortedRDD[K <% Ordered[K]: ClassManifest, V](prev: RDD[(K, V)], ascending: Boolean)
  extends RDD[(K, V)](prev.context) {

  override def splits = prev.splits
  override val partitioner = prev.partitioner
  override val dependencies = List(new OneToOneDependency(prev))

  override def compute(split: Split): Iterator[(K, V)] = {
    val maxBytes = ExternalSorter.getMaxSortBytes
    var inputSize = 0L
    var avgTupleSize = 0L
    var numKVs = 0L
    val input = new ArrayBuffer[(K, V)]
    val inputIter = prev.iterator(split)

    // Take a sample of tuple size after loading 5000 KV's into memory
    while (inputSize < maxBytes) {
      if (inputIter.hasNext) {
        input.append(inputIter.next())
        numKVs += 1
        if (numKVs % 500000 == 0) {
          inputSize = SizeEstimator.estimate(input)
          avgTupleSize =  inputSize / numKVs
        } else {
          inputSize += avgTupleSize
        } 
      } else {
        return input.sortWith((x, y) => if (ascending) x._1 < y._1 else x._1 > y._1).iterator
      }
    }
      
    // Start external sorting.
    val keyClass = implicitly[ClassManifest[K]].erasure
    if (keyClass.isPrimitive) {
      return ExternalSorter.bucketSort(input, inputIter, maxBytes, ascending)
    } else {
      return ExternalSorter.mergeSort(input, inputIter, maxBytes, ascending)
    }
  }
}
