package org.gbif.pipelines.labs.performance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Must not be a part of main build")
public class AvroCompressionTest {

  private static final String BASEPATH = "data/datasets/";
  private static final String RESULTPATH = "results.csv";
  private static final int REPETITION = 1;
  private Integer[] syncIntervals = {1024 * 1024, 2048 * 1024};

  @Test
  public void compressionTest() throws IOException {
    AvroCompressionTestUtility.runCompressionTest(BASEPATH, RESULTPATH, REPETITION,syncIntervals);
    Assert.assertTrue(Files.exists(new File(RESULTPATH).toPath()));
  }
}
