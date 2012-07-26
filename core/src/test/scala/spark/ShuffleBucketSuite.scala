package spark

import org.scalatest.FunSuite
import SparkContext._


import spark.ExternalSorter
import spark.SizeEstimator
import scala.collection.mutable.ArrayBuffer

class InternalBucketSuite extends FunSuite {

  test("Combine (key, value) using put and iterate through combiners") {

  }

  test("Combine (key, combiner) using merge and iterate through combiners") {

  }

  test("Empty input") {

  }
}


class ExternalBucketSuite extends FunSuite {

  test("Write inMemBucket contents to disk during initialization") {

  }

  test("Writes to disk when inMemBucket is full") {

  }

  test("Forces inMemBucket contents to disk before iterating") {

  }

  test("ExternalBucket with more partitions than elements") {

  }

  test("Recursive hashing") {

  }

  test("Negative hashcodes") {

  }
}


class ExternalBucketSuite extends FunSuite {

  test("Implicit conversions for instantiating JavaMap with Object/AnyRef key") {

  }

  test("Default implicit JavaMap instantiation is JavaHashMap") {

  }

}

