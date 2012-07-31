package spark

import org.scalatest.FunSuite
import SparkContext._

import scala.collection.mutable.ArrayBuffer

class ExternalSorterSuite extends FunSuite {

  // Can't use stable ArrayOps.sortWith() to verify unstable mergesort and bucketsort
  def sortCheck(toCheck: Array[(Int, Int)], ascending: Boolean): Boolean = {
    var current = ascending
    for (i <- 1 until toCheck.size) {
      current = current && toCheck(i)._1 >= toCheck(i-1)._1;
      if (ascending != current) {
        return false
      }
    }
    return true
  }

  // Arguments for mergeSort() and bucketSort()
  val rand = new scala.util.Random()
  // 2820 bytes, 20 ArrayBuffer overhead, and each elem is 24(Tuple) + 4(Array ref)
  val dataInMem = ArrayBuffer.fill(1000) { (rand.nextInt(), rand.nextInt()) }
  val dataInMemSize = SizeEstimator.estimate(dataInMem)
  // inputIter iterates through the rest of input that doesn't fit in memory
  val inputIter = ( ArrayBuffer.fill(1000) { (rand.nextInt(), rand.nextInt()) } ).iterator
  val initialTupSize = dataInMemSize/1000

  test("Ascending/descending mergesort w/ replacement selection") {
    val sortedInput = ExternalSorter.mergeSort(
      dataInMem, 
      inputIter, 
      initialTupSize, 
      dataInMemSize, 
      true).toArray
    assert(sortCheck(sortedInput, true))
  }

  test("Descending mergesort w/ replacement selection") {
    val sortedInput = ExternalSorter.mergeSort(
      dataInMem, 
      inputIter, 
      initialTupSize, 
      dataInMemSize, 
      false).toArray
    assert(sortCheck(sortedInput, false))
  }

  test("Mergesort w/ multiple merge passes") {

    import scala.collection.mutable.ArrayBuffer
    import spark._

    // Pass dataInMemSize as maxBytes, so 10 KVs fit in memory
    val dataInMem = new ArrayBuffer[(Int, Int)](10)
    for (i <- 0 until 10) { dataInMem.append((9 - i, 9 - i)) }
    val dataInMemSize = SizeEstimator.estimate(dataInMem)
    // Worst-case for replacement-selection is input in reverse order. 
    // This creates 500 files after the initial pass, so it does 2 merge passes with fanIn = 128.
    // First merge pass outputs 3 merged runs. 
    val input = new ArrayBuffer[(Int, Int)](5000)
    for (i <- 0 until 5000) input.append((4999 - i, 4999 - i))
    val inputIter = input.iterator
    val initialTupSize = dataInMemSize/10
    val sortedInput = ExternalSorter.mergeSort(
      dataInMem, 
      inputIter, 
      initialTupSize, 
      dataInMemSize, 
      true).toArray
    assert(sortCheck(sortedInput, true))
  }

  test("Ascending bucketsort") {
    val sortedInput = ExternalSorter.bucketSort(
      dataInMem, 
      inputIter, 
      initialTupSize, 
      dataInMemSize, 
      true).toArray
    assert(sortCheck(sortedInput, true))
  }

  test("Descending bucketsort") {
    val sortedInput = ExternalSorter.mergeSort(
      dataInMem, 
      inputIter, 
      initialTupSize, 
      dataInMemSize, 
      false).toArray
    assert(sortCheck(sortedInput, false))
  }

  test("Bucketsort with more partitions than elements") {
    // Make sure # of input KVs are even, since half will be in memory, half on disk.
    val numPartitions = ExternalSorter.getNumBuckets
    val numElements = numPartitions/2 + (numPartitions%2)
    val dataInMem = ArrayBuffer.fill(numElements/2) { (rand.nextInt(), rand.nextInt()) }
    val dataInMemSize = SizeEstimator.estimate(dataInMem)
    val inputIter = ( ArrayBuffer.fill(numElements/2) { (rand.nextInt(), rand.nextInt()) } ).iterator
    val initialTupSize = dataInMemSize/numElements
    val sortedInput = ExternalSorter.bucketSort(
      dataInMem, 
      inputIter, 
      initialTupSize, 
      dataInMemSize, 
      true).toArray
    assert(sortCheck(sortedInput, true))
  }

  test("Recursive bucketsort") {
    System.setProperty("spark.externalSorter.numBuckets", "5")
    // Pass dataInMemSize as maxBytes, so 10 KVs fit in memory
    val dataInMem = ArrayBuffer.fill(10) { (rand.nextInt(), rand.nextInt()) }
    val dataInMemSize = SizeEstimator.estimate(dataInMem)
    val initialTupSize = dataInMemSize/10
    // At least one partition file will have more than 10 KVs, which forces recursive bucketsort call.
    val inputIter = ( ArrayBuffer.fill(90) { (rand.nextInt(), rand.nextInt()) } ).iterator
    val sortedInput = ExternalSorter.bucketSort(
      dataInMem, 
      inputIter, 
      initialTupSize, 
      dataInMemSize, 
      true).toArray
    assert(sortCheck(sortedInput, true))
  }
}
