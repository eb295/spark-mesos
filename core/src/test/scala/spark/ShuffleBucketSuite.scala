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
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Boolean])
    assert(createMapFromScalaClass().isInstanceOf[Object2BooleanOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Boolean])
    assert(createMapFromJavaClass().isInstanceOf[Object2BooleanOpenHashMap[_]])
  }

  test("Implicit conversion for Object2ByteOpenHashMap") {
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Byte])
    assert(createMapFromScalaClass().isInstanceOf[Object2ByteOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Byte])
    assert(createMapFromJavaClass().isInstanceOf[Object2ByteOpenHashMap[_]])
  }

  test("Implicit conversion for Object2CharOpenHashMap") {
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Char])
    assert(createMapFromScalaClass().isInstanceOf[Object2CharOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Character])
    assert(createMapFromJavaClass().isInstanceOf[Object2CharOpenHashMap[_]])
  }

  test("Implicit conversion for Object2IntOpenHashMap") {
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Int])
    assert(createMapFromScalaClass().isInstanceOf[Object2IntOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Integer])
    assert(createMapFromJavaClass().isInstanceOf[Object2IntOpenHashMap[_]])
  }

  test("Implicit conversion for Object2DoubleOpenHashMap") {
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Double])
    assert(createMapFromScalaClass().isInstanceOf[Object2DoubleOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Double])
    assert(createMapFromJavaClass().isInstanceOf[Object2DoubleOpenHashMap[_]])
  }

  test("Implicit conversion for Object2FloatOpenHashMap") {
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Float])
    assert(createMapFromScalaClass().isInstanceOf[Object2FloatOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Float])
    assert(createMapFromJavaClass().isInstanceOf[Object2FloatOpenHashMap[_]])
  }

  test("Implicit conversion for Object2LongOpenHashMap") {
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Long])
    assert(createMapFromScalaClass().isInstanceOf[Object2LongOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Long])
    assert(createMapFromJavaClass().isInstanceOf[Object2LongOpenHashMap[_]])
  }

  test("Implicit conversion for Object2ShortOpenHashMap") {
    val createMapFromScalaClass = ShuffleBucket.makeMap(testClass, classOf[Short])
    assert(createMapFromScalaClass().isInstanceOf[Object2ShortOpenHashMap[_]])

    val createMapFromJavaClass = ShuffleBucket.makeMap(testClass, classOf[java.lang.Short])
    assert(createMapFromJavaClass().isInstanceOf[Object2ShortOpenHashMap[_]])
  }

  test("Implicit conversion for Object2ObjectOpenHashMap") {
    val createMap = ShuffleBucket.makeMap(testClass, testClass)
    assert(createMap().isInstanceOf[Object2ObjectOpenHashMap[TestClass, TestClass]])
  }

  test("Default JavaMap instantiation") {
    val createMap = ShuffleBucket.makeMap(classOf[Int], classOf[Int])
    assert(createMap().isInstanceOf[JHashMap[_, _]])
  }
}
