package spark.shuffle

import java.util.{Map => JMap}

import spark.Aggregator

class InternalBucket[K, V, C](
    private val aggregator: Aggregator[K, V, C], 
    private val hashMap: JMap[Any, Any], 
    private val _maxBytes: Long)
  extends ShuffleBucket[K, V, C] {

  def put(key: K, value: V) {
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

  def numCombiners = hashMap.size

  def maxBytes = _maxBytes

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
