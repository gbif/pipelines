package org.gbif.pipelines.clustering;

import lombok.AllArgsConstructor;
import org.apache.spark.Partitioner;
import scala.Tuple2;

/** Partitions by the prefix on the given key extracted from the given key. */
@AllArgsConstructor
class SaltPrefixPartitioner extends Partitioner {
  private final int numPartitions;

  @Override
  public int getPartition(Object key) {
    String k = ((Tuple2<String, String>) key)._1;
    return Integer.parseInt(k.substring(0, k.indexOf(":")));
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }
}
