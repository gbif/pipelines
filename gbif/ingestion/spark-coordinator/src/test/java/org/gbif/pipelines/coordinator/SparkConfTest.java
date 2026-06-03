package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.util.SparkConfUtil.evaluate;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.util.PipelinesConfigUtil;
import org.gbif.pipelines.util.SparkConfUtil;
import org.junit.Assert;
import org.junit.Test;

public class SparkConfTest {

  @Test
  public void testBoundaries() {
    int recordNumber = 1234;

    Assert.assertTrue(evaluate("0 <= recordNumber < 5000", recordNumber));
    Assert.assertFalse(evaluate("5000 <= recordNumber < 50_000", recordNumber));
    Assert.assertTrue(evaluate("1000 <= recordNumber", recordNumber));
    Assert.assertTrue(evaluate("recordNumber < 2000", recordNumber));
    Assert.assertTrue(evaluate("recordNumber > 1000", recordNumber));
    Assert.assertFalse(evaluate("recordNumber > 2000", recordNumber));
    Assert.assertTrue(evaluate("recordCount < 100_000", 99999));
    Assert.assertFalse(evaluate("recordCount < 100_000", 100_001));
  }

  @Test
  public void testExclusiveBoundaries() {
    // Both exclusive: A < x < B
    Assert.assertTrue(evaluate("0 < x < 5000", 1));
    Assert.assertFalse(evaluate("0 < x < 5000", 0)); // lower boundary excluded
    Assert.assertFalse(evaluate("0 < x < 5000", 5000)); // upper boundary excluded
    Assert.assertTrue(evaluate("0 < x < 5000", 4999));

    // Lower exclusive, upper inclusive: A < x <= B
    Assert.assertTrue(evaluate("0 < x <= 5000", 5000)); // upper boundary included
    Assert.assertFalse(evaluate("0 < x <= 5000", 0)); // lower boundary excluded
    Assert.assertTrue(evaluate("0 < x <= 5000", 1));

    // Both inclusive: A <= x <= B
    Assert.assertTrue(evaluate("0 <= x <= 5000", 0)); // lower boundary included
    Assert.assertTrue(evaluate("0 <= x <= 5000", 5000)); // upper boundary included
    Assert.assertFalse(evaluate("0 <= x <= 5000", 5001));

    // Upper inclusive: x <= B
    Assert.assertTrue(evaluate("x <= 5000", 5000));
    Assert.assertFalse(evaluate("x <= 5000", 5001));
    Assert.assertTrue(evaluate("x <= 5000", 4999));

    // Lower inclusive via >=: x >= A
    Assert.assertTrue(evaluate("x >= 1000", 1000));
    Assert.assertFalse(evaluate("x >= 1000", 999));
    Assert.assertTrue(evaluate("x >= 1000", 1001));

    // Reversed compound: B > x > A (same as A < x < B)
    Assert.assertTrue(evaluate("5000 > x > 0", 1));
    Assert.assertFalse(evaluate("5000 > x > 0", 0));
    Assert.assertFalse(evaluate("5000 > x > 0", 5000));

    // Reversed compound both inclusive: B >= x >= A
    Assert.assertTrue(evaluate("5000 >= x >= 0", 0));
    Assert.assertTrue(evaluate("5000 >= x >= 0", 5000));
    Assert.assertFalse(evaluate("5000 >= x >= 0", 5001));
  }

  @Test
  public void test() {

    String testRoot = SparkConfTest.class.getResource("/").getFile();
    ;
    PipelinesConfig pipelinesConfig =
        PipelinesConfigUtil.loadConfig(testRoot + "pipelines-configmap-test.yaml");

    SparkConfUtil.Conf conf1 =
        SparkConfUtil.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 9_000_000, List.of());

    assertEquals(15, conf1.getExecutorInstances());

    SparkConfUtil.Conf conf2 =
        SparkConfUtil.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 1_000_000, List.of());

    assertEquals(15, conf2.getExecutorInstances());

    SparkConfUtil.Conf conf3 =
        SparkConfUtil.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 900_000, List.of());

    assertEquals(10, conf3.getExecutorInstances());

    SparkConfUtil.Conf conf4 =
        SparkConfUtil.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 10_000, List.of());

    assertEquals(0, conf4.getExecutorInstances());

    SparkConfUtil.Conf conf5 =
        SparkConfUtil.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 500_000_000, List.of());

    assertEquals(50, conf5.getExecutorInstances());

    SparkConfUtil.Conf conf6 =
        SparkConfUtil.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 500_000_001, List.of());

    assertEquals(50, conf6.getExecutorInstances());
  }

  @Test
  public void testSmallDatasets() {

    String testRoot = SparkConfTest.class.getResource("/").getFile();
    PipelinesConfig pipelinesConfig =
        PipelinesConfigUtil.loadConfig(testRoot + "pipelines-configmap-test.yaml");
    assertEquals(1, SparkConfUtil.getNumberOfShards(pipelinesConfig, 0L));
    assertEquals(1, SparkConfUtil.getNumberOfShards(pipelinesConfig, 4_999L));
    assertEquals(3, SparkConfUtil.getNumberOfShards(pipelinesConfig, 9_999L));
    assertEquals(5, SparkConfUtil.getNumberOfShards(pipelinesConfig, 10_000L));
    assertEquals(10, SparkConfUtil.getNumberOfShards(pipelinesConfig, 100_000L));
    assertEquals(30, SparkConfUtil.getNumberOfShards(pipelinesConfig, 500_000L));
  }
}
