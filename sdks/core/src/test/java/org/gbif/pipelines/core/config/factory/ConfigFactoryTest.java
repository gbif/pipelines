package org.gbif.pipelines.core.config.factory;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import org.gbif.pipelines.core.config.model.ContentConfig;
import org.gbif.pipelines.core.config.model.KeygenConfig;
import org.gbif.pipelines.core.config.model.KvConfig;
import org.gbif.pipelines.core.config.model.LockConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.junit.Test;

public class ConfigFactoryTest {

  private final String inpPath = getClass().getResource("/pipelines.yaml").getFile();

  @Test
  public void readConfigTest() {

    PipelinesConfig config = ConfigFactory.read(Paths.get(inpPath), PipelinesConfig.class);

    WsConfig gbifApi = config.getGbifApi();
    assertEquals("http://test.test", gbifApi.getWsUrl());
    assertEquals(64L, gbifApi.getCacheSizeMb());
    assertEquals(60L, gbifApi.getTimeoutSec());
    assertEquals(Long.valueOf(500L), gbifApi.getRetryConfig().getInitialIntervalMillis());
    assertEquals(Integer.valueOf(3), gbifApi.getRetryConfig().getMaxAttempts());
    assertEquals(Double.valueOf(1.5d), gbifApi.getRetryConfig().getMultiplier());
    assertEquals(Double.valueOf(0.5d), gbifApi.getRetryConfig().getRandomizationFactor());

    KvConfig nameUsageMatch = config.getNameUsageMatch();
    assertEquals(
        "test11.gbif-test.org,test2.gbif-test.org,test3.gbif-test.org",
        nameUsageMatch.getZkConnectionString());
    assertEquals("test_name_usage_kv", nameUsageMatch.getTableName());
    assertEquals(61L, nameUsageMatch.getWsTimeoutSec());
    assertEquals(65L, nameUsageMatch.getWsCacheSizeMb());
    assertEquals(6, nameUsageMatch.getNumOfKeyBuckets());

    KvConfig geocode = config.getGeocode();
    assertEquals(
        "test12.gbif-test.org,test2.gbif-test.org,test3.gbif-test.org",
        geocode.getZkConnectionString());
    assertEquals("test_geocode_kv", geocode.getTableName());
    assertEquals(62L, geocode.getWsTimeoutSec());
    assertEquals(66L, geocode.getWsCacheSizeMb());
    assertEquals(7, geocode.getNumOfKeyBuckets());

    KvConfig locationFeature = config.getLocationFeature();
    assertEquals(
        "test13.gbif-test.org,test2.gbif-test.org,test3.gbif-test.org",
        locationFeature.getZkConnectionString());
    assertEquals("test_location_feature_kv", locationFeature.getTableName());
    assertEquals(63L, locationFeature.getWsTimeoutSec());
    assertEquals(67L, locationFeature.getWsCacheSizeMb());
    assertEquals(8, locationFeature.getNumOfKeyBuckets());

    WsConfig amplification = config.getAmplification();
    assertEquals("http://amplification.test.test", amplification.getWsUrl());
    assertEquals(64L, amplification.getCacheSizeMb());
    assertEquals(70L, amplification.getTimeoutSec());
    assertEquals(Long.valueOf(500L), amplification.getRetryConfig().getInitialIntervalMillis());
    assertEquals(Integer.valueOf(3), amplification.getRetryConfig().getMaxAttempts());
    assertEquals(Double.valueOf(1.5d), amplification.getRetryConfig().getMultiplier());
    assertEquals(Double.valueOf(0.5d), amplification.getRetryConfig().getRandomizationFactor());

    KeygenConfig keygen = config.getKeygen();
    assertEquals(
        "test16.gbif-test.org,test2.gbif-test.org,test3.gbif-test.org",
        keygen.getZkConnectionString());
    assertEquals("test_occurrence_lookup", keygen.getLookupTable());
    assertEquals("test_occurrence_counter", keygen.getCounterTable());
    assertEquals("test_occurrence", keygen.getOccurrenceTable());

    ContentConfig content = config.getContent();
    assertEquals(69, content.getWsTimeoutSec());
    assertEquals("http://content.test.org:9200/", content.getEsHosts()[0]);

    assertEquals("bitmap/bitmap.png", config.getImageCachePath());

    LockConfig indexLock = config.getIndexLock();
    assertEquals(6, indexLock.getMaxRetries());
    assertEquals(101, indexLock.getSleepTimeMs());
    assertEquals("/indices/", indexLock.getLockingPath());
    assertEquals("occurrence", indexLock.getLockName());
    assertEquals("dev_index_lock", indexLock.getNamespace());
    assertEquals(
        "test14.gbif-test.org,test2.gbif-test.org,test3.gbif-test.org",
        indexLock.getZkConnectionString());

    LockConfig hdfsLock = config.getHdfsLock();
    assertEquals(4, hdfsLock.getMaxRetries());
    assertEquals(99, hdfsLock.getSleepTimeMs());
    assertEquals("/hive/", hdfsLock.getLockingPath());
    assertEquals("hdfsview", hdfsLock.getLockName());
    assertEquals("dev_index_lock", hdfsLock.getNamespace());
    assertEquals(
        "test15.gbif-test.org,test2.gbif-test.org,test3.gbif-test.org",
        hdfsLock.getZkConnectionString());
  }
}
