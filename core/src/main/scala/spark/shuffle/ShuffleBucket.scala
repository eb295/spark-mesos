package spark.shuffle

import it.unimi.dsi.fastutil.objects.{Object2BooleanOpenHashMap, Object2ByteOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2CharOpenHashMap, Object2DoubleOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2FloatOpenHashMap, Object2IntOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap, Object2ObjectOpenHashMap} 
import it.unimi.dsi.fastutil.objects.{Object2ShortOpenHashMap}
import java.util.{HashMap => JHashMap, Map => JMap}

/**
 * Used to combine values and merge combiners in shuffle.
 * Implemented by InternalBucket and ExternalBucket.
 */
trait ShuffleBucket[K, V, C] {
  def put(key: K, value: V)
  def merge(key: K, combiner: C)
  def clear()
  def bucketIterator(): Iterator[(K, C)]
}

/**
 * Contains helpers for Internal/ExternalBucket.
 */
object ShuffleBucket {

  def getNumPartitions = System.getProperty("spark.shuffleBucket.numPartitions", "64").toInt

  def getMaxHashBytes(): Long = {
    val hashMemFractToUse = System.getProperty("spark.shuffleBucket.hashFraction", "0.25").toDouble
    (Runtime.getRuntime.maxMemory * hashMemFractToUse).toLong
  }

  /**
   * Returns a closure for instantiating a Java Map based on key and combiner classes.
   * Specialized Maps available for non-primitive keys.
   */ 
  def makeMap(kClass: Class[_], cClass: Class[_]): () => JMap[Any, Any] = {
    if (kClass.isPrimitive) {
      return () => new JHashMap[Any, Any]
    } else {
      /** Match with Scala and Java primitives, otherwise default to Object2ObjectMap. */
      val createMap = cClass match {
        case c if (c == classOf[Boolean] || c == classOf[java.lang.Boolean]) => 
          () => new Object2BooleanOpenHashMap[Any]
        case c if (c == classOf[Byte] || c == classOf[java.lang.Byte]) => 
          () => new Object2ByteOpenHashMap[Any]
        case c if (c == classOf[Char] || c == classOf[java.lang.Character]) => 
          () => new Object2CharOpenHashMap[Any]
        case c if (c == classOf[Short] || c == classOf[java.lang.Short]) => 
          () => new Object2ShortOpenHashMap[Any]
        case c if (c == classOf[Int] || c == classOf[java.lang.Integer]) => 
          () => new Object2IntOpenHashMap[Any]
        case c if (c == classOf[Long] || c == classOf[java.lang.Long]) => 
          () => new Object2LongOpenHashMap[Any]
        case c if (c == classOf[Float] || c == classOf[java.lang.Float]) => 
          () => new Object2FloatOpenHashMap[Any]
        case c if (c == classOf[Double] || c == classOf[java.lang.Double]) => 
          () => new Object2DoubleOpenHashMap[Any]
        case _ => 
          () => new Object2ObjectOpenHashMap[Any, Any]
      }
      return createMap.asInstanceOf[() => JMap[Any, Any]]
    }
  }
}
