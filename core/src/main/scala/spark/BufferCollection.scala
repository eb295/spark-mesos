package spark

import it.unimi.dsi.fastutil.io.{FastBufferedInputStream, FastBufferedOutputStream}
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.util.{Collections, UUID}

import spark.util.ByteBufferInputStream

/** Wrapper and operations for a list of buffers and files.
 * Used by ExternalSorter.mergeSort() and bucketSort(), and ExternalBucket.
 *
 * @tparam T type of object written to file. Used for casting return Iterator type for
 *           getBufferedIterator().
 * @param op name of the operation using BufferCollection, used in creating temp files.
 * @param numInitialFiles number of initial files to create.
 * @param maxBlockSize maximum size of each buffer block. 
 */
class BufferCollection[T: ClassManifest] (
    private val op: String, 
    private val numInitialFiles: Int, 
    private val maxBlockSize: Long)
  extends Logging {

  private val tmpDir = Utils.createTempDir()
  private val serializer = SparkEnv.get.serializer
  private val blocks = new ObjectArrayList[Block]
  private val bufferSize = BufferCollection.getIOBufferSize()
  for (i <- 0 until numInitialFiles) { addFile() }

  /** Represents a single output buffer block.
   *
   * @param blockBounds byte bounds for each block written to file
   * @param file Java temp file to be written to
   */
  private class Block(var blockBounds: IntArrayList, val file: File) {
    val ser = serializer.newInstance()
    var serializeStream: SerializationStream = _
    var fileStream: FastBufferedOutputStream = _
    var byteStream: FastByteArrayOutputStream = _

    def initOutputStreams() {
      byteStream = new FastByteArrayOutputStream()
      serializeStream = ser.serializeStream(byteStream)
      fileStream = new FastBufferedOutputStream(new FileOutputStream(file), bufferSize)
    }

    def closeOutputStreams() {
      byteStream.close()
      serializeStream.close()
      fileStream.close()
    }
  }

  /**
   * Write object to a buffer block. Flushes the block to disk if its size exceeds maxBlockSize.
   */
  def write(objToWrite: T, fileIndex: Int) {
    val block = blocks.get(fileIndex)
    block.serializeStream.writeObject(objToWrite)
    /* We've filled the block, so write it to file/disk */
    if (block.byteStream.length > maxBlockSize) {
      flushToDisk(block)
    }
  }
  
  /**
   * Writes a buffer to file and updates Block state.
   */
  private def flushToDisk(block: Block) {
    if (block.byteStream.length != 0) {
      try {
        block.byteStream.trim()
        block.fileStream.write(block.byteStream.array)
        block.blockBounds.add(block.byteStream.length)
        block.fileStream.flush()
        block.byteStream.reset()
      } catch {
        case e: Exception => logError("failed to write to file: " + block.file.getName())
      }
    }
  }

  def forceToDisk(fileIndex: Int) = flushToDisk(blocks.get(fileIndex))

  /** Returns a buffered iterator for a file. */
  def getBufferedIterator(fileIndex: Int): Iterator[T] = {
    val block = blocks.get(fileIndex)
    block.fileStream.flush()
    block.closeOutputStreams()
    if (block.blockBounds.size() == 0) {
      return Iterator.empty
    } else {
      return new Iterator[T] {
        var currentBlock = 0
        val block = blocks.get(fileIndex)
        var blocksToRead = block.blockBounds.size()
        val bytes = {
          var maxSoFar = block.blockBounds.get(0)
          var current = block.blockBounds.get(0)
          for (i <- 1 until block.blockBounds.size()) {
           current = block.blockBounds.get(i)
           if (current > maxSoFar) {
             maxSoFar = current
           }
          }
          new Array[Byte](maxSoFar)
        }
        val byteBuffer = ByteBuffer.wrap(bytes)
        val inputStream = new FastBufferedInputStream(new FileInputStream(block.file), bufferSize)
        val deserializeStream = block.ser.deserializeStream(new ByteBufferInputStream(byteBuffer))

        def fetchNextBlock(): Iterator[T] = {
          val byteCount = block.blockBounds.get(currentBlock)
          byteBuffer.rewind()
          inputStream.read(bytes, 0, byteCount)
          byteBuffer.limit(byteCount)
          val blockIter = deserializeStream.toIterator
          currentBlock += 1
          if (currentBlock == blocksToRead) inputStream.close()
          return blockIter.asInstanceOf[Iterator[T]]
        }
  
        var iter = fetchNextBlock()

        def hasNext(): Boolean = iter.hasNext || currentBlock != blocksToRead
      
        def next(): T = {
          if (!iter.hasNext) {
            iter = fetchNextBlock()
          }
          return iter.next()
        }
      }
    }
  }

  /** Resets the file for writing to. */
  def reset(fileIndex: Int) {
    val block = blocks.get(fileIndex)
    block.blockBounds.clear()
    block.initOutputStreams()
  }

  def numFiles = blocks.size
    
  /**
   * Moves the block at oldIndex to newIndex, and deletes
   * the old index.
   */
  def replace(oldIndex: Int, newIndex: Int) {
    val oldBlock = blocks.get(oldIndex)
    blocks.set(newIndex, oldBlock)
    blocks.remove(oldIndex)
  }

  def delete(fileIndex: Int) {
    val block = blocks.get(fileIndex)
    block.closeOutputStreams
    block.file.delete()
    blocks.remove(fileIndex)
  }

  def fitsInMemory(fileIndex: Int, maxBytes: Long): Boolean = {
    val blockBounds = blocks.get(fileIndex).blockBounds
    return (blockBounds.size * maxBlockSize) < maxBytes
  }

  def deleteEmptyFiles() {
    for (i <- numFiles - 1 to 0 by -1) {
      if (blocks.get(i).blockBounds.size == 0) {
        delete(i)
      }
    }
  }

  def clear() = blocks.clear()
  
  def addFile() {
    val file = new File(tmpDir, "spark-" + op + "-" + UUID.randomUUID.toString)
    val blockBounds = new IntArrayList()
    val newBlock = new Block(blockBounds, file)
    newBlock.initOutputStreams()
    blocks.add(newBlock)
  }
}

object BufferCollection {
  def getIOBufferSize(): Int = System.getProperty("spark.externalOps.bufferSize", "8096").toInt
}
