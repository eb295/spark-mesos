package spark

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import SparkContext._

class BufferCollectionSuite extends FunSuite with BeforeAndAfter {

  // BufferCollection uses the serializer from SparkEnv, so we need a SparkContext to initialize that.
  var sc: SparkContext = _

  after {
    if(sc != null) {
      sc.stop()
    }
  }  

  test("Write to buffers, force each to disk and iterate") {
    sc = new SparkContext("local", "test")
    val numFiles = 2
    val bufferSize = 100
    val avgTupSize = 24
    val buffers = new BufferCollection[(Int, Int)]("test", numFiles, bufferSize)
    assert(buffers.numFiles === 2)
    // Write to buffers
    buffers.write((1, 2), 0)
    buffers.write((3, 4), 1)
    // Must force to disk before iterating
    buffers.forceToDisk(0)
    buffers.forceToDisk(1)
    // Check iterators through files 0 and 1
    val bufferedIterArray0 = buffers.getBufferedIterator(0).toArray
    assert(bufferedIterArray0 === Array((1, 2)))
    val bufferedIterArray1 = buffers.getBufferedIterator(1).toArray
    assert(bufferedIterArray1 === Array((3, 4)))
  }

  test("Write out buffer block to file if block is full") {
    sc = new SparkContext("local", "test")
    val buffers = new BufferCollection[(Int, Int)]("test", 1, 13)
    // Write to first buffer
    buffers.write((1, 2), 0)
    assert(buffers.fitsInMemory(0, 13))
    buffers.write((3, 4), 0)
    assert(!buffers.fitsInMemory(0, 13))
    // Force to disk before iterating
    buffers.forceToDisk(0)
    val bufferedIterArray = buffers.getBufferedIterator(0).toArray
    assert(bufferedIterArray === Array((1, 2), (3, 4)))
  }

  test("Empty buffer/file returns empty iterator") {
    sc = new SparkContext("local", "test")
    val buffers = new BufferCollection[(Int, Int)]("test", 1, 100)
    // Force to disk before iterating
    buffers.forceToDisk(0)
    val bufferedIterArray = buffers.getBufferedIterator(0).toArray
    assert(bufferedIterArray.length == 0)
  }

  test("Delete empty buffers/files") {
    sc = new SparkContext("local", "test")
    val buffers = new BufferCollection[(Int, Int)]("test", 4, 100)
    buffers.write((1, 2), 2)
    // Make sure it writes to file
    buffers.forceToDisk(2)
    buffers.deleteEmptyFiles()
    assert(buffers.numFiles == 1)
  }

  test("Replace buffer index") {
    sc = new SparkContext("local", "test")
    val buffers = new BufferCollection[(Int, Int)]("test", 3, 100)
    // Write to first buffer
    buffers.write((1, 2), 0)
    // Replace buffer at index 2 w/ buffer at index 0. This removes old buffer at index 0.
    buffers.replace(0, 2)
    assert(buffers.numFiles === 2)
    // Force to disk before iterating
    buffers.forceToDisk(1)
    val bufferedIterArray = buffers.getBufferedIterator(1).toArray
    assert(bufferedIterArray === Array((1, 2)))
  }
}
