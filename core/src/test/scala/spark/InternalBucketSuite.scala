package spark

import org.scalatest.FunSuite

import java.util.{HashMap => JHashMap, Map => JMap}
import scala.collection.mutable.ArrayBuffer

import SparkContext._
import spark.shuffle.InternalBucket
import spark.shuffle.ShuffleBucket

// ShuffleSuite also tests InternalBucket.
class InternalBucketSuite extends FunSuite {

  // Sort by Key and sort each value
  def sortCombiners(combiners: Array[(Int, ArrayBuffer[Int])]) = {
    combiners.map(kv => (kv._1, kv._2.sorted)).sortBy(_._1)
  }

  def createCombiner(v: Int) = ArrayBuffer(v)
  def mergeValue(buf: ArrayBuffer[Int], v: Int) = buf += v
  def mergeCombiners(b1: ArrayBuffer[Int], b2: ArrayBuffer[Int]) = b1 ++= b2

  val aggregator = new Aggregator[Int, Int, ArrayBuffer[Int]](createCombiner, mergeValue, mergeCombiners)
  val createMap: () => JMap[Any, Any] = () => new JHashMap[Any, Any]
  val maxBytes = ShuffleBucket.getMaxHashBytes

  test("Combine (key, value) using put and iterate through combiners") {
    val bucket = new InternalBucket[Int, Int, ArrayBuffer[Int]](aggregator, createMap(), maxBytes)
    val pairs = Array((1, 1), (1, 2), (2, 1), (1, 3))
    for (kvPair <- pairs) { bucket.put(kvPair._1, kvPair._2) }
    val combined = bucket.bucketIterator().toArray
    assert(sortCombiners(combined) === Array((1, ArrayBuffer(1, 2, 3)),(2, ArrayBuffer(1))))
  }

  test("Combine (key, combiner) using merge and iterate through combiners") {
    val bucket = new InternalBucket[Int, Int, ArrayBuffer[Int]](aggregator, createMap(), maxBytes)
    val combiners = Array((1, ArrayBuffer(1, 2, 3)), (1, ArrayBuffer(4, 5)), (2, ArrayBuffer(6)))
    for (combinerPair <- combiners) { bucket.merge(combinerPair._1, combinerPair._2) }
    val merged = bucket.bucketIterator().toArray
    assert(sortCombiners(merged) === Array((1, ArrayBuffer(1, 2, 3, 4, 5)), (2, ArrayBuffer(6))))
  }

  test("Negative hashcodes") {
    val bucket = new InternalBucket[Int, Int, ArrayBuffer[Int]](aggregator, createMap(), maxBytes)
    val pairs = Array((-1, 1), (1, 2), (2, 1), (-1, 3))
    for (kvPair <- pairs) { bucket.put(kvPair._1, kvPair._2) }
    val combined = bucket.bucketIterator().toArray
    assert(sortCombiners(combined) === Array((-1,ArrayBuffer(1, 3)), (1,ArrayBuffer(2)), (2,ArrayBuffer(1))))
  }

  test("Empty input") {
    val bucket = new InternalBucket[Int, Int, ArrayBuffer[Int]](aggregator, createMap(), maxBytes)
    assert(bucket.bucketIterator.size === 0)
  }
}
