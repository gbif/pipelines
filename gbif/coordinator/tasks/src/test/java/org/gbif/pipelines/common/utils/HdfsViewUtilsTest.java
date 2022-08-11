package org.gbif.pipelines.common.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class HdfsViewUtilsTest {

  @Test
  public void buildOutputPathAsString() {
    assertEquals("1/2/3", HdfsUtils.buildOutputPathAsString("1", "2", "3"));
  }

  @Test
  public void buildOutputPath() {
    assertEquals(new org.apache.hadoop.fs.Path("1/2/3"), HdfsUtils.buildOutputPath("1", "2", "3"));
  }
}
