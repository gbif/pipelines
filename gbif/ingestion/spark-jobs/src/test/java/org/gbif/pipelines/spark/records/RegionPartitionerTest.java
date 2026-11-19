package org.gbif.pipelines.spark.records;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.spark.records.RecordsTable.RegionPartitioner;
import org.junit.Test;

public class RegionPartitionerTest {

  /** Start keys of a table pre-split like the occurrence one: "", "01", ... "99" */
  private static final byte[][] START_KEYS =
      IntStream.range(0, 100)
          .mapToObj(i -> Bytes.toBytes(i == 0 ? "" : String.format("%02d", i)))
          .toArray(byte[][]::new);

  @Test
  public void onePartitionPerRegionForSmallLoads() {
    RegionPartitioner partitioner = new RegionPartitioner(START_KEYS, 10);

    assertEquals(100, partitioner.numPartitions());
    assertEquals(0, partition(partitioner, "00:100"));
    assertEquals(67, partition(partitioner, "67:1234567"));
    assertEquals(99, partition(partitioner, "99:999"));
  }

  @Test
  public void regionsSpreadOverSeveralPartitions() {
    RegionPartitioner partitioner = new RegionPartitioner(START_KEYS, 1200);

    assertEquals(1200, partitioner.numPartitions());

    Set<Integer> used = new HashSet<>();
    for (long id = 0; id < 10_000; id++) {
      String rowKey = "25:" + (id * 100 + 25);
      int partition = partition(partitioner, rowKey);
      // the partitions of region 25
      assertTrue(partition >= 25 * 12 && partition < 26 * 12);
      used.add(partition);
    }
    assertEquals(12, used.size());
  }

  @Test
  public void partitionsFollowTheRecordCount() {
    // small loads keep the input partitions
    assertEquals(40, RecordsTable.hfilePartitions(40, 1_000, 500_000));
    // large ones get enough partitions to stay under the records per partition
    assertEquals(6_000, RecordsTable.hfilePartitions(40, 3_000_000_000L, 500_000));
    assertEquals(3, RecordsTable.hfilePartitions(1, 1_000_001, 500_000));
  }

  private static int partition(RegionPartitioner partitioner, String rowKey) {
    return partitioner.getPartition(rowKey);
  }
}
