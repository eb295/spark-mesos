package spark.scheduler

import java.io._
import java.util.HashMap
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import com.ning.compress.lzf.LZFInputStream
import com.ning.compress.lzf.LZFOutputStream

import spark._
import spark.shuffle.ShuffleBucket
import spark.shuffle.InternalBucket
import spark.shuffle.ExternalBucket
import spark.storage._


object ShuffleMapTask {
  val serializedInfoCache = new HashMap[Int, Array[Byte]]
  val deserializedInfoCache = new HashMap[Int, (RDD[_], ShuffleDependency[_,_,_])]

  def serializeInfo(stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_,_]): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId)
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
        objOut.writeObject(rdd)
        objOut.writeObject(dep)
        objOut.close()
        val bytes = out.toByteArray
        serializedInfoCache.put(stageId, bytes)
        return bytes
      }
    }
  }

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], ShuffleDependency[_,_,_]) = {
    synchronized {
      val old = deserializedInfoCache.get(stageId)
      if (old != null) {
        return old
      } else {
        val loader = Thread.currentThread.getContextClassLoader
        val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
        val objIn = new ObjectInputStream(in) {
          override def resolveClass(desc: ObjectStreamClass) =
            Class.forName(desc.getName, false, loader)
        }
        val rdd = objIn.readObject().asInstanceOf[RDD[_]]
        val dep = objIn.readObject().asInstanceOf[ShuffleDependency[_,_,_]]
        val tuple = (rdd, dep)
        deserializedInfoCache.put(stageId, tuple)
        return tuple
      }
    }
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
      deserializedInfoCache.clear()
    }
  }
}

class ShuffleMapTask(
    stageId: Int,
    var rdd: RDD[_], 
    var dep: ShuffleDependency[_,_,_],
    var partition: Int, 
    @transient var locs: Seq[String])
  extends Task[BlockManagerId](stageId)
  with Externalizable
  with Logging {

  def this() = this(0, null, null, 0, null)
  
  var split = if (rdd == null) {
    null 
  } else { 
    rdd.splits(partition)
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeInt(stageId)
    val bytes = ShuffleMapTask.serializeInfo(stageId, rdd, dep)
    out.writeInt(bytes.length)
    out.write(bytes)
    out.writeInt(partition)
    out.writeObject(split)
  }

  override def readExternal(in: ObjectInput) {
    val stageId = in.readInt()
    val numBytes = in.readInt()
    val bytes = new Array[Byte](numBytes)
    in.readFully(bytes)
    val (rdd_, dep_) = ShuffleMapTask.deserializeInfo(stageId, bytes)
    rdd = rdd_
    dep = dep_
    partition = in.readInt()
    split = in.readObject().asInstanceOf[Split]
  }

  override def run(attemptId: Long): BlockManagerId = {
    val numOutputSplits = dep.partitioner.numPartitions
    val aggregator = dep.aggregator.asInstanceOf[Aggregator[Any, Any, Any]]
    val partitioner = dep.partitioner.asInstanceOf[Partitioner]
    val maxBytes = ShuffleBucket.getMaxHashBytes
    var usingExternalHash = false
    var bytesUsed = 0L
    val bucketSize = maxBytes/numOutputSplits
    val pairRDDIter = rdd.iterator(split).asInstanceOf[Iterator[(Any, Any)]]
    var buckets: Array[ShuffleBucket[Any, Any, Any]]= Array.tabulate(numOutputSplits)(_=> 
      new InternalBucket(aggregator, dep.createMap()))

    var numInserted = 0
    var avgObjSize = 0L
    
    while(pairRDDIter.hasNext) {
      val (k, v) = pairRDDIter.next()
      var bucketId = partitioner.getPartition(k)
      val bucket = buckets(bucketId)
      bucket.put(k, v)
      numInserted += 1
      if (numInserted == 5000) {
        bytesUsed = SizeEstimator.estimate(buckets)
        avgObjSize = bytesUsed/5000
      }
      if (!usingExternalHash && bytesUsed > maxBytes) {
        buckets = buckets.map(bucket =>
          new ExternalBucket(
            bucket.asInstanceOf[InternalBucket[Any, Any, Any]],
            numInserted,
            avgObjSize,
            bucketSize))
        usingExternalHash = true
      } else {
        bytesUsed += avgObjSize
      }
    }

    val ser = SparkEnv.get.serializer.newInstance()
    val blockManager = SparkEnv.get.blockManager
    for (i <- 0 until numOutputSplits) {
      val blockId = "shuffleid_" + dep.shuffleId + "_" + partition + "_" + i
      val iter = buckets(i).bucketIterator()
      val storageLvl = {
        if (usingExternalHash) {
          StorageLevel.DISK_AND_MEMORY 
        } else {
          StorageLevel.MEMORY_ONLY
        }
      }
      blockManager.put(blockId, iter, storageLvl, false)
    }
    return SparkEnv.get.blockManager.blockManagerId
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partition)
}
