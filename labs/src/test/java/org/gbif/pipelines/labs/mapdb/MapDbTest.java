package org.gbif.pipelines.labs.mapdb;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

import com.google.common.base.Stopwatch;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests different configurations to use mapdb.
 */
@Ignore("Must not be a part of main build")
public class MapDbTest {

  private static final Logger LOG = LoggerFactory.getLogger(MapDbTest.class);

  private static final long MEMORY_SIZE_10MB = 10L * 1024L * 1024L;
  private static final long MEMORY_SIZE_10GB = 10L * 1024L * 1024L * 1024L;

  @Test
  public void givenHybridMapWithLowMemoryWhenMappedThenMoreRecordsInDiskThanInMemTest() {

    DB dbDisk = DBMaker.tempFileDB().fileMmapEnableIfSupported().executorEnable().make();

    DB dbMemory = DBMaker.memoryDirectDB().executorEnable().make();

    // Big map populated with data expired from cache
    HTreeMap onDiskMap = createDiskMap(dbDisk);
    HTreeMap<String, String> onMemoryMap = createMemoryMap(dbMemory, onDiskMap, 0, 2);

    // we simulate several fragments of records assuming 1 thread only
    for (int fragment = 0; fragment < 10000; fragment++) {
      // create the fragment
      int n = 5000;

      // add each record of the fragment to the mapCache
      for (int i = 0; i < n; i++) {
        onMemoryMap.putIfAbsentBoolean(UUID.randomUUID().toString(), String.valueOf(i));
      }
    }

    Assert.assertTrue(onDiskMap.getSize() > onMemoryMap.getSize());
  }

  @Test
  public void hybridMapPerformanceBasedOnMemorySizeTest() {

    DB dbDisk = createDbDisk();

    DB dbMemory = createDbMemory();
    HTreeMap onDiskMap = createDiskMap(dbDisk);

    long memorySize = MEMORY_SIZE_10GB;

    HTreeMap<String, String> onMemoryMap = createMemoryMap(dbMemory, onDiskMap, memorySize, 2);

    // we simulate several fragments of records assuming 1 thread only
    List<Long> timesMeasured = mapAndMeasureTime(onMemoryMap);
    LOG.info(String.valueOf(memorySize + " memory: " + timesMeasured.stream()
      .mapToDouble(x -> x)
      .average()
      .getAsDouble()));
  }

  @Test
  public void hybridMapPerformanceBasedOnExecutorPoolSizeTest() {

    DB dbDisk = createDbDisk();

    DB dbMemory = createDbMemory();
    HTreeMap onDiskMap = createDiskMap(dbDisk);

    int numThreads = 5;

    // using low memory to have more data transfer from memory to disk
    HTreeMap<String, String> onMemoryMap = createMemoryMap(dbMemory, onDiskMap, MEMORY_SIZE_10MB, numThreads);

    // we simulate several fragments of records assuming 1 thread only
    List<Long> timesMeasured = mapAndMeasureTime(onMemoryMap);
    LOG.info(String.valueOf(numThreads + " executors: " + timesMeasured.stream()
      .mapToDouble(x -> x)
      .average()
      .getAsDouble()));
  }

  @NotNull
  private List<Long> mapAndMeasureTime(HTreeMap<String, String> onMemoryMap) {
    int nFragments = 2000;
    List<Long> timesMeasured = new ArrayList<>(nFragments);
    for (int fragment = 0; fragment < nFragments; fragment++) {
      int n = 100;

      // add each record of the fragment to the mapCache
      Stopwatch stopwatch = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
        onMemoryMap.putIfAbsentBoolean(UUID.randomUUID().toString(), String.valueOf(i));
      }
      timesMeasured.add(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));
    }
    return timesMeasured;
  }

  @SuppressWarnings("unchecked")
  @NotNull
  private HTreeMap<String, String> createMemoryMap(DB dbMemory, HTreeMap onDiskMap, long memorySize, int poolSize) {
    // fast in-memory collection with limited size
    return (HTreeMap<String, String>) dbMemory.hashMap("inMemory")
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING_ASCII)
      .expireStoreSize(memorySize)
      .expireAfterCreate()
      // registers overflow to onDiskMap
      .expireOverflow(onDiskMap)
      // enables background expiration
      .expireExecutor(Executors.newScheduledThreadPool(poolSize))
      //.expireExecutorPeriod(10000)
      .createOrOpen();
  }

  @NotNull
  private DB createDbMemory() {
    return DBMaker.memoryDirectDB().transactionEnable().executorEnable().make();
  }

  @NotNull
  private HTreeMap createDiskMap(DB dbDisk) {
    // Big map populated with data expired from cache
    return dbDisk.hashMap("onDisk")
      .keySerializer(Serializer.STRING)
      .valueSerializer(Serializer.STRING_ASCII)
      .createOrOpen();
  }

  @NotNull
  private DB createDbDisk() {
    return DBMaker.tempFileDB().fileMmapEnableIfSupported().transactionEnable().executorEnable().make();
  }

}
