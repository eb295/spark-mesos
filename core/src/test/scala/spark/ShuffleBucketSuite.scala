package spark

import it.unimi.dsi.fastutil.objects.{Object2BooleanOpenHashMap, Object2ByteOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2CharOpenHashMap, Object2DoubleOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2FloatOpenHashMap, Object2IntOpenHashMap}
import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap, Object2ObjectOpenHashMap} 
import it.unimi.dsi.fastutil.objects.{Object2ShortOpenHashMap}
import java.util.{HashMap => JHashMap, Map => JMap}

import org.scalatest.FunSuite
import SparkContext._
import spark.shuffle.ShuffleBucket

// Test implicit conversions. In each test, the first case uses a Scala primitive 
// and the second uses the corresponding Java primitive.
class ShuffleBucketSuite extends FunSuite {

  class TestClass {}

  val testClass = classOf[TestClass]

  test("Implicit conversion for Object2BooleanOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Boolean])
    assert(createMap().isInstanceOf[Object2BooleanOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Boolean])
    assert(createMap().isInstanceOf[Object2BooleanOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2ByteOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Byte])
    assert(createMap().isInstanceOf[Object2ByteOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Byte])
    assert(createMap().isInstanceOf[Object2ByteOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2CharOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Char])
    assert(createMap().isInstanceOf[Object2CharOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Character])
    assert(createMap().isInstanceOf[Object2CharOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2IntOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Int])
    assert(createMap().isInstanceOf[Object2IntOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Integer])
    assert(createMap().isInstanceOf[Object2IntOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2DoubleOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Double])
    assert(createMap().isInstanceOf[Object2DoubleOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Double])
    assert(createMap().isInstanceOf[Object2DoubleOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2FloatOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Float])
    assert(createMap().isInstanceOf[Object2FloatOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Float])
    assert(createMap().isInstanceOf[Object2FloatOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2LongOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Long])
    assert(createMap().isInstanceOf[Object2LongOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Long])
    assert(createMap().isInstanceOf[Object2LongOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2ShortOpenHashMap") {
    var createMap = ShuffleBucket.makeMap(testClass, classOf[Short])
    assert(createMap().isInstanceOf[Object2ShortOpenHashMap[Any]])

    createMap = ShuffleBucket.makeMap(testClass, classOf[java.lang.Short])
    assert(createMap().isInstanceOf[Object2ShortOpenHashMap[Any]])
  }

  test("Implicit conversion for Object2ObjectOpenHashMap") {
    val createMap = ShuffleBucket.makeMap(testClass, testClass)
    assert(createMap().isInstanceOf[Object2ObjectOpenHashMap[TestClass, TestClass]])
  }

  test("Default JavaMap instantiation") {
    val createMap = ShuffleBucket.makeMap(classOf[Int], classOf[Int])
    assert(createMap().isInstanceOf[JHashMap[Any, Any]])
  }
}
