package spark;

import it.unimi.dsi.fastutil.io.{FastBufferedInputStream, FastBufferedOutputStream}
import it.unimi.dsi.fastutil.objects.ObjectArrayList

import java.io.File
import java.io.{FileInputStream,FileOutputStream}
import java.io.{EOFException,IOException}
import java.io.ObjectOutputStream
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

class BufferCollection[T: ClassManifest](
    val op: String, 
    var numInitialBuffers: Int, 
    val maxBufferSize: Long,
    var avgObjSize: Long = 0L) extends Logging {

  private val tmpDir = Utils.createTempDir()
  private val ser = SparkEnv.get.serializer.newInstance()
  private val fileBuffers = new ObjectArrayList[FileBuffer]
  for (i <- 0 until numInitialBuffers) {
    fileBuffers.add(new FileBuffer(0L, 0, new ArrayBuffer[Any], 
      new File(tmpDir, "spark-" + op + "-" + UUID.randomUUID.toString)))
  }
   
  class FileBuffer(
      var size: Long, 
      var blocksWritten: Int, 
      val buffer: ArrayBuffer[Any], 
      val file: File) {
    val serializeStream = ser.serializeStream(
      new FastBufferedOutputStream(new FileOutputStream(file)))
  }
  
  def write(toWrite: Any, bufferNum: Int, size: Long = avgObjSize, measureSize: Boolean = true) {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.buffer.append(toWrite)
    fileBuffer.size += size
    if (fileBuffer.size > maxBufferSize) writeToDisk(fileBuffer, measureSize)
  }
  
  def writeToDisk(fileBuffer: FileBuffer, measureSize: Boolean) {
    try {     
      if (!fileBuffer.buffer.isEmpty) {
        // Update the avg object size
        if (measureSize) {
          avgObjSize = (SizeEstimator.estimate(fileBuffer.buffer.toArray)
            /fileBuffer.buffer.size + avgObjSize) / 2
        }
        val out = fileBuffer.serializeStream
        out.writeObject(fileBuffer.buffer)
        fileBuffer.buffer.clear()
        fileBuffer.size = 0
        fileBuffer.blocksWritten += 1
      }
    } catch {
      case e: IOException =>
      logWarning("failed to write to file: " + fileBuffer.file.getName())
    }
  }
  
  def forceToDisk(bufferNum: Int, measureSize: Boolean = true) = 
    writeToDisk(fileBuffers.get(bufferNum), measureSize)

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

  def getBufferIterator(bufferNum: Int): Iterator[T] = {
    return new Iterator[T] {
      val blockIter = getBlockIterator(bufferNum)
      var bufferIter = blockIter.next().iterator

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

  def clear(bufferNum: Int) = {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.buffer.clear()
    fileBuffer.blocksWritten = 0
    fileBuffer.size = 0
  }

  def numBuffers = fileBuffers.size
  
  def move(oldBufferNum: Int, newBufferNum: Int) = {
    val oldBuffer = fileBuffers.get(oldBufferNum)
    fileBuffers.set(newBufferNum, oldBuffer)
    fileBuffers.remove(oldBufferNum)
  }

  def delete(bufferNum: Int) = {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.file.delete()
    fileBuffers.remove(bufferNum)
  }

  def fitsInMemory(bufferNum: Int, maxBytes: Long): Boolean = {
    val blocksWritten = fileBuffers.get(bufferNum).blocksWritten
    return (blocksWritten*maxBufferSize) < maxBytes
  }

  def deleteEmptyFiles() = {
    for (i <- numBuffers - 1 to 0 by -1) {
      if (fileBuffers.get(i).blocksWritten == 0) {
        val fileBuffer = fileBuffers.get(i)
        fileBuffer.file.delete()
        fileBuffer.buffer.clear()
        fileBuffers.remove(i)
      }
    }
  }
  
  def addBuffer(bufferSize: Long = maxBufferSize, index: Int = numBuffers) {
    val newBuffer = new FileBuffer(0L, 0, new ArrayBuffer[Any], 
      new File(tmpDir, "spark-" + op + "-" + UUID.randomUUID.toString))
    fileBuffers.add(newBuffer)
  }
}
