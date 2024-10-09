package org.gbif.pipelines.common.hdfs;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HdfsViewSettings {

  /** Computes number of file shards: */
  public static int computeNumberOfShards(AvroWriteConfiguration avroConfig, long recordsNumber) {
    double shards = recordsNumber / (double) avroConfig.recordsPerAvroFile;
    shards = Math.max(shards, 1d);
    boolean isCeil =
        (shards - Math.floor(shards)) > 0.49d; // Floor if extra shard size is less than 49%
    return isCeil ? (int) Math.ceil(shards) : (int) Math.floor(shards);
  }
}
