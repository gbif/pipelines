package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.airflow.AirflowConfFactory.evaluate;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.gbif.pipelines.airflow.AirflowConfFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.util.PipelinesConfigUtil;
import org.junit.Assert;
import org.junit.Test;

public class AirflowFactoryConfTest {

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
  public void test() {

    String testRoot = AirflowFactoryConfTest.class.getResource("/").getFile();
    ;
    PipelinesConfig pipelinesConfig =
        PipelinesConfigUtil.loadConfig(testRoot + "pipelines-configmap-test.yaml");

    AirflowConfFactory.Conf conf1 =
        AirflowConfFactory.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 9_000_000, List.of());

    assertEquals(15, conf1.getExecutorInstances());

    AirflowConfFactory.Conf conf2 =
        AirflowConfFactory.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 1_000_000, List.of());

    assertEquals(15, conf2.getExecutorInstances());

    AirflowConfFactory.Conf conf3 =
        AirflowConfFactory.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 900_000, List.of());

    assertEquals(10, conf3.getExecutorInstances());

    AirflowConfFactory.Conf conf4 =
        AirflowConfFactory.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 10_000, List.of());

    assertEquals(2, conf4.getExecutorInstances());

    AirflowConfFactory.Conf conf5 =
        AirflowConfFactory.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 500_000_000, List.of());

    assertEquals(50, conf5.getExecutorInstances());

    AirflowConfFactory.Conf conf6 =
        AirflowConfFactory.createConf(
            pipelinesConfig, "dataset-uuid", 1, "testAppName", 500_000_001, List.of());

    assertEquals(50, conf6.getExecutorInstances());
  }
}
