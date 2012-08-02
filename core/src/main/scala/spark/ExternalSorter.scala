package spark

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.PriorityQueue

/** Contains external sorting implementations and helpers. */
object ExternalSorter {

  def getFanIn = System.getProperty("spark.externalSorter.fanIn", "128").toInt

  def getNumBuckets = System.getProperty("spark.externalSorter.numBuckets", "64").toInt

  def getMaxSortBytes: Long = {
    val SortMemFractToUse = System.getProperty("spark.externalSorter.sortFraction", "0.25").toDouble
    (Runtime.getRuntime.maxMemory * SortMemFractToUse).toLong
  }

  /**
   * External mergesort implementation, using replacement-selection to generate runs.
   *
   * @tparam K type of keys to be sorted by
   * @tparam V type of value
   * @param dataInMem holds tuples already buffered in memory (size > maxBytes).
   * @param inputIter iterator for remaining tuples to be read.
   * @param initialTupSize average tuple size for tuples in the dataInMem buffer.
   * @param maxBytes maximum in-memory bytes to use for sorting.
   * @param ascending true if ascending sort order.
   *
   * @return Iterator for (K, V) tuples, sorted by K.
   */
  def mergeSort[K <% Ordered[K], V](
      dataInMem: ArrayBuffer[(K, V)], 
      inputIter: Iterator[(K, V)], 
      initialTupSize: Long,
      maxBytes: Long,
      ascending: Boolean): Iterator[(K, V)] = {
    
    /** Max fan-in for each merge run */
    val fanIn = ExternalSorter.getFanIn
    val bufferSize = maxBytes/fanIn
    val op = "ReplacementSelection-MergeSort"
  
    /**
     * Replacement-selection to generate runs in pass 0. Uses maxBytes + bufferSize memory.
     *
     * @return BufferCollection of buffers/files that have been written to.
     */
    def generateRuns(): BufferCollection[((K, Int), V)] = {
      /** 16L min. overhead for ((K, Int), V) tuple: tuple ref(4) + Int(4) + min object size(8) */
      val fileBuffers = new BufferCollection[((K, Int), V)](op, 1, bufferSize, initialTupSize + 16L)
      var currentSet = new PriorityQueue[((K, Int), V)]()(KeyRunOrdering)
      currentSet ++= dataInMem.map(KV => ((KV._1, 0), KV._2))
      var currentRun = 0
  
      /** Write out a KV to the current run, or start a new run. */
      def writeToRun(output: ((K, Int), V)) {
        val outputRun = output._1._2
        if (outputRun != currentRun) {
          fileBuffers.forceToDisk(currentRun)
          currentRun += 1
          fileBuffers.addBuffer()
        }
        fileBuffers.write(output, currentRun)
      }
      
      /**
       * Write to output run. An input KV will be part of a new run if it is smaller than the
       * KV to be written.
       */
      while (inputIter.hasNext) {
        for ((key, value) <- inputIter) {
          var output = currentSet.dequeue
          val (outputKey, outputRun) = output._1 
          writeToRun(output)

          val newInputRun = {
            if (key < outputKey) {
              outputRun + 1 
            } else {
              outputRun
            }
          }
          val newInput = ((key, newInputRun), value)
          currentSet.enqueue(newInput)
        }
      }
  
      /**
       * Finished reading all KV's from input iterator. Write KVs in the current set
       * to disk for the last run, or the last two runs.
       */
      while (currentSet.size != 0) {
        var output = currentSet.dequeue
        writeToRun(output)
      }
      /** Force the last output buffer to disk. At this point all input KVs have been written out. */
      fileBuffers.forceToDisk(currentRun)

      fileBuffers
    }
  
    /** Merges files written to in generateRuns(). Number of files to merge = fileBuffers.numBuffers. */
    def mergeRuns(fileBuffers: BufferCollection[((K, Int), V)]): Iterator[((K, Int), V)] = {
      var numMergePasses = math.ceil(math.log(fileBuffers.numBuffers)/math.log(fanIn))
      val mergeQueue = new PriorityQueue[((K, Int), V)]()(KeyOrdering)

      /**
       * A single merge run.
       *
       * @param start index of first file to merge.
       * @param numToMerge number of files to merge.
       * @param curentOutputRun index of output file.
       * @param firstRun true if this is the first merge run in a pass.
       * @return number of files successfully merged, should be equal to numToMerge (sanity check).
       */
      def mergeRun(start: Int, numToMerge: Int, currentOutputRun: Int, firstRun: Boolean): Int = {
        val inputBufferIters = new Array[Iterator[((K, Int), V)]](numToMerge)
        var runsMerged = 0
        mergeQueue.clear()

        /** Get input file iterators and enqueue a KV from each into the current set. */
        for (i <- start until start + numToMerge) {
          val bufferIter = fileBuffers.getBufferedIterator(i)
          if (bufferIter.hasNext) mergeQueue.enqueue(bufferIter.next())
          inputBufferIters(i - start) = bufferIter
        }

        while (!mergeQueue.isEmpty) {
          val ((key, inputRun), value) = mergeQueue.dequeue()
          val output = {
            if (firstRun) {
              ((key, 0), value)
            } else {
              ((key, currentOutputRun), value)
            }
          }
          fileBuffers.write(output, currentOutputRun)
          /** Check if there are elements left in the input file. If so, enqueue. */
          if (inputBufferIters(inputRun - start).hasNext) {
            mergeQueue.enqueue(inputBufferIters(inputRun - start).next())
          } else {
            runsMerged += 1
            fileBuffers.reset(inputRun)
          }
        }
        /** Finished merging. Force output buffer to disk. */
        fileBuffers.forceToDisk(currentOutputRun)

        return runsMerged
      }

      /** Each iteration is a pass. */
      while (numMergePasses > 0) {
        /** Total files to merge for current pass. */
        var totalToMerge = fileBuffers.numBuffers
        /** Files to merge for first merge run */
        var numToMerge = math.min(totalToMerge, fanIn)
        /** Index of first file to merge. */
        var start = 0
        /** Add one temp file to hold the first merge run output. */
        var currentOutputRun = fileBuffers.numBuffers
        fileBuffers.addBuffer()
        var firstRun = true

        /** Each iteration is a merge run. */
        while (totalToMerge > 0) {
          val merged = mergeRun(start, numToMerge, currentOutputRun, firstRun)

          /** Move the first merge run output file from the last index to index 0. */
          if (firstRun) {
            fileBuffers.replace(currentOutputRun, 0)
            currentOutputRun = 1
            firstRun = false
          } else {
            currentOutputRun += 1
          }

          start = start + merged
          totalToMerge -= merged
          numToMerge = math.min(totalToMerge, fanIn)
        }

        fileBuffers.deleteEmptyFiles()
        numMergePasses -= 1
      }
      return fileBuffers.getBufferedIterator(0)
    }

    /** Sort by run first, then key. For the priority queue used in pass 0. */
    implicit object KeyRunOrdering extends Ordering[((K, Int), V)] {
      def compare(x: ((K, Int), V), y: ((K, Int), V)) = {
        val (xKey, xRun) = x._1
        val (yKey, yRun) = y._1
        val runCompare = yRun.compare(xRun)
        if (runCompare == 0) {
          if (ascending) yKey.compare(xKey) else xKey.compare(yKey)
        } else {
          runCompare
        }
      }
    }
  
    /** Sort by key. For the priority queue used to merge. */
    implicit object KeyOrdering extends Ordering[((K, Int), V)] {
      def compare(x: ((K, Int), V), y: ((K, Int), V)) = {
        val xKey = x._1._1
        val yKey = y._1._1
        if (ascending) yKey.compare(xKey) else xKey.compare(yKey)
      }
    }
  
    return new Iterator[(K, V)] {
      val iter = mergeRuns(generateRuns)

      def hasNext() = iter.hasNext

      def next() = {
        val ((key, _), value) = iter.next()
        (key, value)
      }
    }
  }
    
  
  /**
   * External bucketsort implementation, using Scala's quicksort to sort each bucket.
   *
   * @param dataInMem a buffer that holds tuples already iterated through in memory (size > maxBytes).
   * @param inputIter iterator for remaining tuples to be sorted.
   * @param initialTupSize average tuple size of tuples in dataInMem buffer.
   * @param maxBytes maximum in-memory bytes to use for sorting.
   * @param ascending a boolean to specify sort order.
   *
   * @return a (K, V) iterator, sorted by K.
   */
  def bucketSort[K <% Ordered[K]: ClassManifest, V](
      dataInMem: ArrayBuffer[(K, V)], 
      inputIter: Iterator[(K, V)],
      initialTupSize: Long,
      maxBytes: Long, 
      ascending: Boolean): Iterator[(K, V)] = {
    
    val numBuckets = ExternalSorter.getNumBuckets
    val bufferSize = maxBytes/numBuckets
    val op = "BucketSort"
    val fileBuffers = 
      new BufferCollection[(K, V)](op, numBuckets, bufferSize, initialTupSize)
    
    /** Sample the dataInMem to get range bounds. */
    val rangeBounds: Array[K] = {
      val dataSize = dataInMem.length
      val maxSampleSize = numBuckets * 10.0
      val frac = math.min(maxSampleSize / math.max(dataSize, 1), 1.0)
      var sample = {
        val rg = new Random(1)
        val oldData = dataInMem.iterator.map(_._1).toArray
        val sampleSize = (oldData.size * frac).ceil.toInt
        for (i <- 1 to sampleSize) yield oldData(rg.nextInt(dataInMem.size))
      }
      sample = sample.sortWith((x, y) => x < y )
      if (sample.length == 0) {
        Array()
      } else {
        val bounds = new Array[K](numBuckets)
        for (i <- 0 until numBuckets) {
          bounds(i) = sample(i * sample.length / numBuckets)
        }
        bounds
      }
    }
    
    def getBucket(key: K): Int = {
      var bucket = 0
      while (bucket < rangeBounds.length - 1 && key > rangeBounds(bucket)) {
        bucket += 1
      }
      if (ascending) {
        bucket
      } else {
        rangeBounds.length - 1 - bucket
      }
    }
  
    /** Write in-memory KVs to disk. */
    for (kv <- dataInMem) {
      val bucket = getBucket(kv._1)
      fileBuffers.write(kv, bucket)
    }
    dataInMem.clear()

    /** Write remaining KVs to disk. */
    while (inputIter.hasNext) {
      val outputKV = inputIter.next()
      val bucket = getBucket(outputKV._1)
      fileBuffers.write(outputKV, bucket)
    }

    /** Must force everything to disk before iterating. */
    for (i <- 0 until numBuckets) fileBuffers.forceToDisk(i)

    /** Delete bucket files that haven't been written to */
    fileBuffers.deleteEmptyFiles()
    
    return new Iterator[(K, V)] {
      var bucketToRead = 0
      val buckets = fileBuffers.numBuffers

      /** Load a bucket into memory and return sorted iterator.
       * Recursively call bucketSort() if bucket contents don't fit in memory.
       */
      def loadNextIter(): Iterator[(K, V)] = {
        val toRet = {
          if (fileBuffers.fitsInMemory(bucketToRead, maxBytes)) {
            val block = fileBuffers.getBufferedIterator(bucketToRead).toArray
            block.sortWith((x, y) => if (ascending) x._1 < y._1 else x._1 > y._1).iterator
          } else {
            val bufferIter = fileBuffers.getBufferedIterator(bucketToRead)
            var size = 0L
            while(bufferIter.hasNext && size < maxBytes) {
              dataInMem.append(bufferIter.next())
              size += fileBuffers.avgObjSize
            }
            bucketSort(dataInMem, bufferIter, fileBuffers.avgObjSize, maxBytes, ascending)
          }
        }
        /** If we've read all buckets from disk, delete all files */
        bucketToRead += 1
        if (bucketToRead == buckets) fileBuffers.clear()
        return toRet
      }


      var inputIter = loadNextIter()

      def hasNext() = inputIter.hasNext || bucketToRead < buckets

      def next(): (K, V) = {
        if (!inputIter.hasNext) {
          inputIter = loadNextIter()
        }
        inputIter.next()
      }

      def toArray = inputIter.toArray
    }
  }
}
