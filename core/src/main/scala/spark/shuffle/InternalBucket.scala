package spark.shuffle

import java.util.{Map => JMap}
import scala.collection.JavaConversions._

import spark.Aggregator

/**
 * Encapsulates in-memory hash maps.
 * Initialized in ShuffledRDD and ShuffleMapTask.
 *
 * @param aggregator an Aggregator for K, V, C types passed.
 * @param hashMap a Java Map that holds (K, C) entries. K <: Object and V <: AnyVal pairs have
 *                specialized Maps, detected in ShuffleBucket.makeMap(), called in ShuffledRDD.
 */
class InternalBucket[K, V, C](
    private val aggregator: Aggregator[K, V, C], 
    private val hashMap: JMap[K, C])
  extends ShuffleBucket[K, V, C] {

  def put(key: K, value: V) {
    val existing = hashMap.get(key)
    if (existing == null) {
      hashMap.put(key, aggregator.createCombiner(value))
    } else {
      hashMap.put(key, aggregator.mergeValue(existing, value))
    }
  }

  def merge(key: K, c: C) {
    val existing = hashMap.get(key)
    if (existing == null) {
      hashMap.put(key, c)
    } else {
      hashMap.put(key, aggregator.mergeCombiners(c, existing))
    }
  }

  def clear() = hashMap.clear()

  def numCombiners = hashMap.size

  def bucketIterator() = hashMap.iterator
}
