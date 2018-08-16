package org.koeninger.spark.citus

import scala.collection.mutable.ArrayBuffer
import java.nio.{ ByteBuffer, ByteOrder }

import org.apache.spark.{ Partition, Partitioner }
import scalikejdbc._
import xbird.util.hashes.JenkinsHash

/**
 * Mapping from citus partition to spark partition and vice versa.
 * Assumes all citus tables have equal number of partitions, one placement per shard,
 * and that shard key is the same.
 * @param placements should already be sorted by tablename, shardmaxvalue asc
 */
class CitusPartitioner(placements: Array[ShardPlacement]) extends Partitioner {
  /** placements should already be sorted, so we'll make them 1:1 with spark partitions */
  val numPartitions = placements.size

  /** @param key should be a tuple of (table: String, key: Int), or an instance of CitusKey */
  override def getPartition(key: Any): Int = {
    key match {
      case (table: String, k: Int) =>
        findPartition32(table, k)
      case (table: String, k: Long) =>
        findPartition64(table, k)
      case (table: String, k: String) =>
        findPartitionString32(table, k)
      case k: CitusKey =>
        findPartition32(k.table, k.key)
    }
  }

  // need a quick way to determine partition index, so use lookup arrays for table and bucket
  // overall index into placements == (table index * number of buckets) + bucket index
  // a given bucket is bounded by shardminvalue and shardmaxvalue inclusive, but we're going to do
  // linear search from the beginning, so we only need to keep track of shardmaxvalue
  private val (tables, buckets) = {
    var i = 0
    // only building one array of buckets, based on the first table in placements
    var finishedBuckets = false
    val tableBuf = new ArrayBuffer[String]()
    val bucketBuf = new ArrayBuffer[Long]()
    placements.foreach { p =>
      if (tableBuf.isEmpty) {
        tableBuf.append(p.tableName)
      }
      if (p.tableName != tableBuf.last) {
        finishedBuckets = true
        tableBuf.append(p.tableName)
        i = 0
      }
      if (!finishedBuckets) {
        bucketBuf.append(p.shardMaxValue)
      } else {
        assert(bucketBuf(i) == p.shardMaxValue, "tables must have the same shard buckets")
      }
      i = i + 1
    }

    val buckets = bucketBuf.toArray
    val tables = tableBuf.toArray
    assert(placements.size / tables.size.toDouble == buckets.size, "tables must have the same number of shards")
    assert(tables.size == tables.toSet.size, "tables must not be repeated out of order")
    assert(tables.sorted.toSeq == tables.toSeq, "placements must be sorted by table")
    assert(buckets.sorted.toSeq == buckets.toSeq, "placements must be sorted by table, then shardmaxvalue")
    // Remove for now to handle Longs
    //assert(buckets.last == Int.MaxValue, "shardmaxvalue should top out at Int.MaxValue")
    (tables, buckets)
  }

  /** set of jdbc worker hosts, in (host, port) form */
  val jdbcWorkerHosts: Set[(String, Int)] = placements.map(p => (p.nodeName, p.nodePort)).toSet

  // Needed for hashing
  // Not sure whether a threadlocal for this is faster than just allocating a new one each time
  @transient private var byteBuf32: ThreadLocal[ByteBuffer] = null

  private def getByteBuf32(): ByteBuffer = {
    if (null == byteBuf32) {
      byteBuf32 = new ThreadLocal[ByteBuffer] {
        override def initialValue() = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      }
    }
    byteBuf32.get
  }

  @transient private var byteBuf64: ThreadLocal[ByteBuffer] = null

  private def getByteBuf64(): ByteBuffer = {
    if (null == byteBuf64) {
      byteBuf64 = new ThreadLocal[ByteBuffer] {
        override def initialValue() = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      }
    }
    byteBuf64.get
  }

  /** given an already hashed value, find the index of the bucket that value should fall in */
  def findBucket32(hashed: Int): Int = {
    var i = 0
    // This only needs a single conditional check because Int.MaxValue is a sentinel at the end of the buckets array
    // Because it's only a single branch, should be close in perf to a binary search for up to 64 ~ 128 buckets
    while (hashed > buckets(i)) {
      i = i + 1
    }
    i
  }

  def findBucket64(hashed: Long): Int = {
    var i = 0
    // This only needs a single conditional check because Int.MaxValue is a sentinel at the end of the buckets array
    // Because it's only a single branch, should be close in perf to a binary search for up to 64 ~ 128 buckets
    while (hashed > buckets(i)) {
      i = i + 1
    }
    i
  }

  /** given a table name and UNhashed value, find the partition aka placement index */
  def findPartition32(tableName: String, unhashed: Int): Int = {
    val bb = getByteBuf32
    bb.clear
    val hashed = JenkinsHash.postgresHashint4(bb.putInt(unhashed).array)

    (tables.indexOf(tableName) * buckets.size) + findBucket32(hashed)
  }

  def findPartitionString32(tableName: String, unhashed: String): Int = {
    val unhashed_bytes = unhashed.getBytes
    val bb = ByteBuffer.allocate(unhashed_bytes.length).order(ByteOrder.LITTLE_ENDIAN)
    bb.clear

    val hashed = JenkinsHash.postgresHashString32(bb.put(unhashed_bytes).array)

    (tables.indexOf(tableName) * buckets.size) + findBucket32(hashed)
  }

  def findPartition64(tableName: String, unhashed: Long): Int = {
    val bb = getByteBuf64
    bb.clear
    val hashed = JenkinsHash.postgresHashint8(bb.putLong(unhashed).array)

    (tables.indexOf(tableName) * buckets.size) + findBucket64(hashed)
  }

  /** given a partition ID, return the correct shard placement */
  def shardPlacement(partitionId: Int): ShardPlacement = placements(partitionId)
}

object CitusPartitioner {
  /**
   * Convenience constructor for a CitusPartitioner.
   * This is using scalikejdbc library for db interaction.
   * If you want to use some other library, or raw jdbc, submit a PR :)
   * You can also just do the query yourself and call new CitusPartitioner.
   * @param session an open scalikejdbc session to the Citus master node
   */
  def apply(tables: Seq[String])(implicit session: DBSession): CitusPartitioner = {
    val placements = sql"""
select
 (ds.logicalrelid::regclass)::varchar as tablename,
 ds.shardmaxvalue::bigint as hmax,
 ds.shardid::integer,
 p.nodename::varchar,
 p.nodeport::integer
from pg_dist_shard ds
 left join pg_dist_shard_placement p on ds.shardid = p.shardid
where (ds.logicalrelid::regclass)::varchar in ($tables)
order by tablename, hmax asc
""".map { rs =>
      ShardPlacement(rs.string(1), rs.long(2), rs.int(3), rs.string(4), rs.int(5))
    }.list.apply()

    assert(placements.map(_.tableName).toSet == tables.toSet, "Not all tables were present in pg_dist_shard_placement")

    new CitusPartitioner(placements.toArray)
  }

}
