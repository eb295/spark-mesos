package spark

import it.unimi.dsi.fastutil.io.{FastBufferedInputStream, FastBufferedOutputStream}
import it.unimi.dsi.fastutil.objects.ObjectArrayList

import java.io.File
import java.io.{FileInputStream,FileOutputStream}
import java.io.{EOFException,IOException}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer

/** Wrapper and operations for a list of buffers and files.
 * Used by ExternalSorter.mergeSort() and bucketSort(), and ExternalBucket.
 *
 * @tparam T type of object written to file. Used for casting return Iterator type for
 *           getBufferedIterator().
 * @param op name of the operation using BufferCollection, used in creating temp files.
 * @param numInitialBuffers number of initial files to create.
 * @param maxBufferSize maximum size of each buffer.
 * @param avgObjSize initial average size of each object to be written out.
 */
class BufferCollection[T: ClassManifest](
    private val op: String, 
    private val numInitialBuffers: Int, 
    private val maxBufferSize: Long,
    private var _avgObjSize: Long) extends Logging {

  private val tmpDir = Utils.createTempDir()
  private val ser = SparkEnv.get.serializer.newInstance()
  private val fileBuffers = new ObjectArrayList[FileBuffer]
  for (i <- 0 until numInitialBuffers) { addBuffer() }

  /** Represents a single output buffer.
   *
   * @param size byte size of the current buffer in memory.
   * @param blocksWritten number of buffers written to file
   * @paran buffer contains objects to be written to file
   * @param file Java temp file to be written to
   */
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
  
  /**
   * Write object to a buffer. Writes the buffer to disk if its size exceeds maxBufferSize.
   */
  def write(toWrite: Any, bufferNum: Int) {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.buffer.append(toWrite)
    fileBuffer.size += _avgObjSize
    if (fileBuffer.size > maxBufferSize) writeToDisk(fileBuffer)
  }
  
  /**
   * Writes a buffer to file. Updates FileBuffer variables(state), and the avgObjSize of
   * objects written out.
   */
  private def writeToDisk(fileBuffer: FileBuffer) {
    if (!fileBuffer.buffer.isEmpty) {
      try {     
        _avgObjSize = (SizeEstimator.estimate(fileBuffer.buffer.toArray)
                       /fileBuffer.buffer.size + _avgObjSize) / 2
        val out = fileBuffer.serializeStream
        out.writeObject(fileBuffer.buffer)
        fileBuffer.buffer.clear()
        fileBuffer.size = 0
        fileBuffer.blocksWritten += 1
      } catch {
        case e: Exception => logError("failed to write to file: " + fileBuffer.file.getName())
      }
    }
  }

  def forceToDisk(bufferNum: Int) = writeToDisk(fileBuffers.get(bufferNum))

  /**
   * Helper method that returns an iterator over blocks of elements written to a file.
   * This only iterates through elements written to file, so forceToDisk() must be
   * called beforehand.
   */
  private def getBlockIterator(bufferNum: Int): Iterator[ArrayBuffer[T]] = {
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

  /** Returns a block-buffered iterator for a file. */
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

  /** Resets the FileBuffer for writing to. */
  def reset(bufferNum: Int) {
    val fileBuffer = fileBuffers.get(bufferNum)
    fileBuffer.buffer.clear()
    fileBuffer.initSerializeStream
    fileBuffer.blocksWritten = 0
    fileBuffer.size = 0
  }

  def numBuffers = fileBuffers.size
  
  def avgObjSize = _avgObjSize
  
  /**
   * Sets the FileBuffer at index oldBufferNum to index newBufferNum, and deletes
   * the old list index.
   */
  def replace(oldBufferNum: Int, newBufferNum: Int) {
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
