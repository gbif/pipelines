package org.gbif.pipelines.common.hdfs;

import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class HdfsViewSettingsTest {

  @Test
  public void computeLessThanOneTest() {

    AvroWriteConfiguration configuration = new AvroWriteConfiguration();
    configuration.recordsPerAvroFile = 20_000_000L;

    long records = 10_000_000L;

    int result = HdfsViewSettings.computeNumberOfShards(configuration, records);

    Assert.assertEquals(1, result);
  }

  @Test
  public void computeOneAndHalfTest() {

    AvroWriteConfiguration configuration = new AvroWriteConfiguration();
    configuration.recordsPerAvroFile = 20_000_000L;

    long records = 29_000_000L;

    int result = HdfsViewSettings.computeNumberOfShards(configuration, records);

    Assert.assertEquals(1, result);
  }

  @Test
  public void computeTwoShardsTest() {

    AvroWriteConfiguration configuration = new AvroWriteConfiguration();
    configuration.recordsPerAvroFile = 20_000_000L;

    long records = 45_000_000L;

    int result = HdfsViewSettings.computeNumberOfShards(configuration, records);

    Assert.assertEquals(2, result);
  }
}
