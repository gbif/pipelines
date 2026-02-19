package org.gbif.pipelines.common.process;

import static org.junit.Assert.*;

import org.gbif.pipelines.common.configs.SparkConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkDynamicSettingsTest {

  private static final SparkConfiguration CONFIG = new SparkConfiguration();

  @BeforeClass
  public static void setUp() {
    CONFIG.executorMemoryGbMin = 8;
    CONFIG.executorMemoryGbMax = 70;
    CONFIG.executorInstancesMin = 2;
    CONFIG.executorInstancesMax = 70;
    // Power function setting
    CONFIG.powerFnCoefficient = 0.000138d;
    CONFIG.powerFnExponent = 0.626d;
    CONFIG.powerFnMemoryCoef = 2.8d;
    CONFIG.powerFnExecutorCoefficient = 1d;
    CONFIG.memoryExtraCoef = 1.3d;
  }

  @Test
  public void r100RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 100;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(CONFIG.executorInstancesMin, sparkSettings.getExecutorNumbers());
    assertEquals(CONFIG.executorMemoryGbMin, sparkSettings.getExecutorMemory());
  }

  @Test
  public void m1RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 1_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(CONFIG.executorInstancesMin, sparkSettings.getExecutorNumbers());
    assertEquals(CONFIG.executorMemoryGbMin, sparkSettings.getExecutorMemory());
  }

  @Test
  public void m10RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 10_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(4, sparkSettings.getExecutorNumbers());
    assertEquals(10, sparkSettings.getExecutorMemory());
  }

  @Test
  public void m10RecordsExtraMemorySettingsTest() {

    // State
    long fileRecordsNumber = 10_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, true);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(4, sparkSettings.getExecutorNumbers());
    assertEquals(13, sparkSettings.getExecutorMemory());
  }

  @Test
  public void m30RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 30_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(7, sparkSettings.getExecutorNumbers());
    assertEquals(19, sparkSettings.getExecutorMemory());
  }

  @Test
  public void m100RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 100_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(15, sparkSettings.getExecutorNumbers());
    assertEquals(40, sparkSettings.getExecutorMemory());
  }

  @Test
  public void m300RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 300_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(28, sparkSettings.getExecutorNumbers());
    assertEquals(70, sparkSettings.getExecutorMemory());
  }

  @Test
  public void m500RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 500_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(39, sparkSettings.getExecutorNumbers());
    assertEquals(70, sparkSettings.getExecutorMemory());
  }

  @Test
  public void b12RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 1_277_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(CONFIG.executorInstancesMax, sparkSettings.getExecutorNumbers());
    assertEquals(CONFIG.executorMemoryGbMax, sparkSettings.getExecutorMemory());
  }

  @Test
  public void b15RecordsSettingsTest() {

    // State
    long fileRecordsNumber = 1_500_000_000;

    // When
    SparkDynamicSettings sparkSettings =
        SparkDynamicSettings.create(CONFIG, fileRecordsNumber, false);

    // Should
    assertNotNull(sparkSettings);
    assertEquals(CONFIG.executorInstancesMax, sparkSettings.getExecutorNumbers());
    assertEquals(CONFIG.executorMemoryGbMax, sparkSettings.getExecutorMemory());
  }
}
