package spark

import org.scalatest.FunSuite
import SparkContext._

class BufferCollectionSuite extends FunSuite {
  test("Write to buffers, force each to disk and iterate") {
    val numBuffers = 2
    val bufferSize = 100
    val avgTupSize = 24
    val buffers = new BufferCollection("test", numBuffers, bufferSize, avgTupSize)
    assert(buffers.numBuffers === 2)
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

  test("Empty buffer/file returns empty iterator") {
    val buffers = new BufferCollection("test", 1, 100, 24)
    // Force to disk before iterating
    buffers.forceToDisk(0)
    val bufferedIterArray = buffers.getBufferedIterator(0).toArray
    assert(bufferedIterArray.length == 0)
  }

  test("Write buffer to file when full") {
    val buffers = new BufferCollection("test", 1, 30, 24)
    // Write to first buffer
    buffers.write((1, 2), 0)
    assert(buffers.fitsInMemory(0, 30))
    buffers.write((3, 4), 0)
    assert(!buffers.fitsInMemory(0, 30))
    // Force to disk before iterating
    buffers.forceToDisk(0)
    val bufferedIterArray = buffers.getBufferedIterator(0).toArray
    assert(bufferedIterArray === Array((1, 2), (3, 4)))
  }

  test("Delete empty buffers/files") {
    val buffers = new BufferCollection("test", 4, 100, 24)
    buffers.write((1, 2), 2)
    // Make sure it writes to file
    buffers.forceToDisk(2)
    buffers.deleteEmptyFiles()
    assert(buffers.numBuffers == 1)
  }

  test("Replace buffer index") {
    val buffers = new BufferCollection("test", 3, 100, 24)
    // Write to first buffer
    buffers.write((1, 2), 0)
    // Replace buffer at index 2 w/ buffer at index 0. This removes old buffer at index 0.
    buffers.replace(0, 2)
    assert(buffers.numBuffers === 2)
    // Force to disk before iterating
    buffers.forceToDisk(1)
    val bufferedIterArray = buffers.getBufferedIterator(1).toArray
    assert(bufferedIterArray === Array((1, 2)))
  }
}
