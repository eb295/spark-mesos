package spark

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.PriorityQueue

import scala.runtime.ScalaRunTime._


object ExternalSorter {
  def getFanIn = System.getProperty("spark.sortedRDD.fanIn", "128").toInt
  def getNumBuckets = System.getProperty("spark.sortedRDD.numBuckets", "64").toInt

  def getMaxSortBytes: Long = {
    val SortMemFractToUse = System.getProperty("spark.sortedRDD.sortFraction", "0.25").toDouble
    (Runtime.getRuntime.maxMemory * SortMemFractToUse).toLong
  }

  def mergeSort[K <% Ordered[K], V](
      dataInMem: ArrayBuffer[(K, V)], 
      inputIter: Iterator[(K, V)], 
      initialTupSize: Long,
      maxBytes: Long,
      ascending: Boolean): Iterator[(K, V)] = {
    
    // This will be the max fan-in during merge 
    val fanIn = ExternalSorter.getFanIn
    val bufferSize = maxBytes/fanIn
    val op = "ReplacementSelection-MergeSort"
  
    // Replacement selection for pass 0. It uses maxMem + bufferSize memory. 
    def generateRuns() = {
      var currentSet = new PriorityQueue[((K, Int), V)]()(KeyRunOrdering)
      // Minimum overhead for another tuple: tuple ref(4) + Int(4) + min object size(8)
      val fileBuffers = new BufferCollection[((K, Int), V)](op, 1, bufferSize, initialTupSize + 16L)
      currentSet ++= dataInMem.map(KV => ((KV._1, 0), KV._2))
      var currentRun = 0
  
      // Write out a KV to the current run, or start a new one
      def writeToRun(output: ((K, Int), V)) = {
        val outputRun = output._1._2
        if (outputRun != currentRun) {
          // Force output buffer to disk. 
          fileBuffers.forceToDisk(currentRun)
          // Add a new file buffer for new run
          currentRun += 1
          fileBuffers.addBuffer()
        }
        fileBuffers.write(output, currentRun)
      }
      
      // Load values into input buffer and begin generating runs
      while (inputIter.hasNext) {
        for ((key, value) <- inputIter) {
          // Dequeue and write to output buffer
          var output = currentSet.dequeue
          val (outputKey, outputRun) = output._1 
          writeToRun(output)

          // Enqueue the next input KV. If it's smaller than the KV to be written out,
          // make it part of a new run.
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
  
      // Finished reading all KV's from input iterator. Write KVs in the
      // current set to disk for the last run, or the last two runs.
      while (currentSet.size != 0) {
        var output = currentSet.dequeue
        writeToRun(output)
      }
      // Force the last output buffer to disk
      fileBuffers.forceToDisk(currentRun)

      fileBuffers
    }
  
    def mergeRuns(fileBuffers: BufferCollection[((K, Int), V)]): Iterator[((K, Int), V)] = {
      var numMergePasses = math.ceil(math.log(fileBuffers.numBuffers)/math.log(fanIn))
      val mergeQueue = new PriorityQueue[((K, Int), V)]()(KeyOrdering)

      def mergeRun(start: Int, numToMerge: Int, currentOutputRun: Int, firstRun: Boolean): Int = {
        val inputBufferIters = new Array[Iterator[((K, Int), V)]](numToMerge)
        var runsMerged = 0
        mergeQueue.clear()

        // Get input file iterators, enqueue KVs into the current set.
        for (i <- start until start + numToMerge) {
          val bufferIter = fileBuffers.getBufferedIterator(i)
          if (bufferIter.hasNext) mergeQueue.enqueue(bufferIter.next())
          inputBufferIters(i - start) = bufferIter
        }
        while (!mergeQueue.isEmpty) {
          val ((key, inputRun), value) = mergeQueue.dequeue()
          // Update the output run #
          val output = {
            if (firstRun) {
              ((key, 0), value)
            } else {
              ((key, currentOutputRun), value)
	    }
	  }
          fileBuffers.write(output, currentOutputRun)
          // Check if there are elements left in the input file. If so, enqueue.
          if (inputBufferIters(inputRun - start).hasNext) {
            mergeQueue.enqueue(inputBufferIters(inputRun - start).next())
          } else {
            runsMerged += 1
            fileBuffers.reset(inputRun)
          }
        }
        // Finished merging. Force buffer to disk.
        fileBuffers.forceToDisk(currentOutputRun)

        return runsMerged
      }

      // Start merge passes
      while (numMergePasses > 0) {
        var totalToMerge = fileBuffers.numBuffers
        // Files to merge for the each merge
        var numToMerge = math.min(totalToMerge, fanIn)
        // Starting fileBuffers index for each merge run.
        var start = 0
        // Add one buffer as a temp to hold the first merge output
        var currentOutputRun = fileBuffers.numBuffers
        fileBuffers.addBuffer()
        var firstRun = true

        // Merge totalToMerge files for this pass
        while (totalToMerge > 0) {
          val merged = mergeRun(start, numToMerge, currentOutputRun, firstRun)

          // If first merge run of this pass just finished, move it from the end to to index 0
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

        // Delete empty files and update merge count
        fileBuffers.deleteEmptyFiles()
        numMergePasses -= 1
      }
      return fileBuffers.getBufferedIterator(0)
    }

    // Sort by run first, then key. Used for pass 0.
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
  
    // Sort by key. Used for merging
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
    
  
  // Sorting using buckets, uses rangePartitioner to partition into local bucket files
  def bucketSort[K <% Ordered[K]: ClassManifest, V](
      dataInMem: ArrayBuffer[(K, V)], 
      inputIter: Iterator[(K, V)],
      initialTupSize: Long,
      maxBytes: Long, 
      ascending: Boolean): Iterator[(K, V)] = {
    
    val rangePartitions = ExternalSorter.getNumBuckets
    val bufferSize = maxBytes/rangePartitions
    val op = "BucketSort"
    val fileBuffers = 
      new BufferCollection[(K, V)](op, rangePartitions, bufferSize, initialTupSize)
    
    // Sample the dataInMem to get range bounds
    val rangeBounds: Array[K] = {
      val dataSize = dataInMem.length
      val maxSampleSize = rangePartitions * 10.0
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
        val bounds = new Array[K](rangePartitions)
        for (i <- 0 until rangePartitions) {
          bounds(i) = sample(i * sample.length / rangePartitions)
        }
        bounds
      }
    }
    
    def getPartition(key: K): Int = {
      var partition = 0
      while (partition < rangeBounds.length - 1 && key > rangeBounds(partition)) {
        partition += 1
      }
      if (ascending) {
        partition
      } else {
        rangeBounds.length - 1 - partition
      }
    }
  
    // Pass 0, partition into buckets
    // Take care of KVs already in mem
    for (kv <- dataInMem) {
      val part = getPartition(kv._1)
      fileBuffers.write(kv, part)
    }
    dataInMem.clear()

    while (inputIter.hasNext) {
      val outputKV = inputIter.next()
      val part = getPartition(outputKV._1)
      fileBuffers.write(outputKV, part)
    }

    // Force everything to disk.
    for (i <- 0 until fileBuffers.numBuffers) fileBuffers.forceToDisk(i)

    // Delete files that haven't been written to
    fileBuffers.deleteEmptyFiles()
    
    return new Iterator[(K, V)] {
      var buffersRead = 0
      val buffersToRead = fileBuffers.numBuffers

      // Load the next partition into the in-memory hash table, and return its iterator.
      def loadNextIter(): Iterator[(K, V)] = {
        //Recursively hash if it doesn't fit in memory. 
        buffersRead += 1
        val toRet = {
          if (fileBuffers.fitsInMemory(0, maxBytes)) {
            val blockIter = fileBuffers.getBlockIterator(0)
            val block = blockIter.next()
            block.sortWith((x, y) => if (ascending) x._1 < y._1 else x._1 > y._1).iterator
          } else {
            val bufferIter = fileBuffers.getBufferedIterator(0)
            var size = 0L
            while(bufferIter.hasNext && size < maxBytes) {
              dataInMem.append(bufferIter.next())
              size += fileBuffers.avgObjSize
            }
            bucketSort(dataInMem, bufferIter, fileBuffers.avgObjSize, maxBytes, ascending)
          }
        }
        // Delete file once we've iterator through its contents
        fileBuffers.delete(0)
        return toRet
      }


      var inputIter = loadNextIter()

      def hasNext() = inputIter.hasNext || buffersRead < buffersToRead

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
