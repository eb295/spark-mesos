package spark

import it.unimi.dsi.fastutil.io.{FastBufferedInputStream, FastBufferedOutputStream}
import it.unimi.dsi.fastutil.objects.ObjectArrayList

import java.io.File
import java.io.{FileInputStream,FileOutputStream}
import java.io.{EOFException,IOException}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

/** A wrapper for an ArrayList of output buffers and files
 *
 * @param op 
 * @param numInitialBuffers
 * @param maxBufferSize
 * @param avgObjSize
 */
class BufferCollection[T: ClassManifest](
    private val op: String, 
    private var numInitialBuffers: Int, 
    private val maxBufferSize: Long,
    private var _avgObjSize: Long) extends Logging {

  private val tmpDir = Utils.createTempDir()
  private val ser = SparkEnv.get.serializer.newInstance()
  private val fileBuffers = new ObjectArrayList[FileBuffer]
  for (i <- 0 until numInitialBuffers) { addBuffer() }

  private class FileBuffer(
      var size: Long, 
      var blocksWritten: Int, 
      val buffer: ArrayBuffer[Any], 
      val file: File) {
    var serializeStream: SerializationStream = _

    def initSerializeStream() {
      serializeStream = ser.serializeStream(
        new FastBufferedOutputStream(new FileOutputStream(file)))
    }
  }
  
  def write(toWrite: Any, bufferNum: Int) {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.buffer.append(toWrite)
    fileBuffer.size += _avgObjSize
    if (fileBuffer.size > maxBufferSize) writeToDisk(fileBuffer)
  }
  
  private def writeToDisk(fileBuffer: FileBuffer) {
    try {     
      if (!fileBuffer.buffer.isEmpty) {
        // Update the avg object size
        _avgObjSize = (SizeEstimator.estimate(fileBuffer.buffer.toArray)
          /fileBuffer.buffer.size + _avgObjSize) / 2
        val out = fileBuffer.serializeStream
        out.writeObject(fileBuffer.buffer)
        fileBuffer.buffer.clear()
        fileBuffer.size = 0
        fileBuffer.blocksWritten += 1
      }
    } catch {
      case e: Exception => logError("failed to write to file: " + fileBuffer.file.getName())
    }
  }
  
  def forceToDisk(bufferNum: Int) = writeToDisk(fileBuffers.get(bufferNum))

  // Creates an iterator that returns an ArrayBuffer of elements T for each next() call
  def getBlockIterator(bufferNum: Int): Iterator[ArrayBuffer[T]] = {
    return new Iterator[ArrayBuffer[T]] {
      val fileBuffer = fileBuffers.get(bufferNum)
      fileBuffer.serializeStream.close()

      val in = ser.deserializeStream(
        new FastBufferedInputStream(new FileInputStream(fileBuffer.file)))

      var blocksLeft = fileBuffer.blocksWritten
      
      def hasNext(): Boolean = (blocksLeft != 0)

      def next(): ArrayBuffer[T] = {
        try {
          val ret = in.readObject().asInstanceOf[ArrayBuffer[T]]
      	  blocksLeft -= 1
      	  if (blocksLeft == 0) in.close()
      	  return ret
      	} catch {
      	  case e: Exception => logError("failed to read from file " + fileBuffer.file.getName())
        }
      	null
      }
    }
  }

  def getBufferedIterator(bufferNum: Int): Iterator[T] = {
    return new Iterator[T] {
      val blockIter = getBlockIterator(bufferNum)
      var bufferIter = if (blockIter.hasNext) blockIter.next().iterator else Iterator.empty

      def hasNext(): Boolean = blockIter.hasNext || bufferIter.hasNext

      def next(): T = {
      	if (!bufferIter.hasNext) {
      	  bufferIter = blockIter.next().iterator
      	}
      	bufferIter.next()
      }

      def toArray = bufferIter.toArray
    }
  }

  def reset(bufferNum: Int) {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.buffer.clear()
    fileBuffer.initSerializeStream
    fileBuffer.blocksWritten = 0
    fileBuffer.size = 0
  }

  def numBuffers = fileBuffers.size
  
  def avgObjSize = _avgObjSize
  
  def replace(oldBufferNum: Int, newBufferNum: Int) = {
    val oldBuffer = fileBuffers.get(oldBufferNum)
    fileBuffers.set(newBufferNum, oldBuffer)
    fileBuffers.remove(oldBufferNum)
  }

  def delete(bufferNum: Int) {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.file.delete()
    fileBuffers.remove(bufferNum)
    Unit
  }

  def fitsInMemory(bufferNum: Int, maxBytes: Long): Boolean = {
    val blocksWritten = fileBuffers.get(bufferNum).blocksWritten
    return (blocksWritten * maxBufferSize) < maxBytes
  }

  def deleteEmptyFiles() {
    for (i <- numBuffers - 1 to 0 by -1) {
      if (fileBuffers.get(i).blocksWritten == 0) {
        delete(i)
      }
    }
  }
  
  def addBuffer(index: Int = numBuffers) {
    val newBuffer = new FileBuffer(0L, 0, new ArrayBuffer[Any], 
      new File(tmpDir, "spark-" + op + "-" + UUID.randomUUID.toString))
    newBuffer.initSerializeStream
    fileBuffers.add(newBuffer)
  }
}
