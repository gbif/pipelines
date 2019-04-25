package org.gbif.pipelines.keygen;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.gbif.pipelines.keygen.config.OccHbaseConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Note not a real JUnit test, but an extremely expensive performance test that should use the real cluster.
 */
public class HBaseLockingKeyServiceThroughputTest {

  private static final OccHbaseConfiguration CFG = new OccHbaseConfiguration();

  static {
    CFG.setEnvironment("keygen_test");
  }

  private Connection connection;
  private final HBaseLockingKeyService keyService;

  private static final AtomicInteger keysGenerated = new AtomicInteger(0);

  public HBaseLockingKeyServiceThroughputTest(int hbasePoolSize) throws IOException {
    Configuration hBaseConfiguration = HBaseConfiguration.create();
    hBaseConfiguration.set("hbase.hconnection.threads.max", Integer.toString(hbasePoolSize));
    connection = ConnectionFactory.createConnection(hBaseConfiguration);
    keyService = new HBaseLockingKeyService(CFG, connection);
  }

  public void testNoContention(int threadCount) throws InterruptedException {
    // test generating ids as fast as possible in the ideal case of no waiting for contention (all ids are globally
    // unique)
    int genPerThread = 100000;
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < threadCount; i++) {
      Thread thread = new Thread(new KeyGenerator(keyService, UUID.randomUUID(), genPerThread));
      thread.start();
      threads.add(thread);
    }

    Thread rateReporter = new Thread(new RateReporter(threadCount));
    rateReporter.start();

    for (Thread thread : threads) {
      thread.join();
    }

    rateReporter.interrupt();
    rateReporter.join();
  }

  private static class RateReporter implements Runnable {

    private final int threadCount;

    private RateReporter(int threadCount) {
      this.threadCount = threadCount;
    }

    @Override
    public void run() {
      int periods = 0;
      int runningAvg = 0;
      int buildAverageAfter = 15;
      int lastCount = 0;
      boolean interrupted = false;
      while (!interrupted) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          interrupted = true;
        }
        int generated = keysGenerated.intValue() - lastCount;
        if (periods > buildAverageAfter) {
          if (runningAvg == 0) {
            runningAvg = generated;
          } else {
            int netPeriods = periods - buildAverageAfter;
            runningAvg = (netPeriods * runningAvg + generated) / (netPeriods + 1);
          }
          System.out.println("Key generation at [" + generated + " keys/s] for running avg of [" + runningAvg
              + " keys/s] and per thread [" + (runningAvg / threadCount)
              + " keys/sec] with id generation time of [" + (threadCount * 1000 / runningAvg)
              + " ms/id]");
        } else {
          System.out.println("Stats in [" + (buildAverageAfter - periods) + "] seconds.");
        }
        periods++;
        lastCount = keysGenerated.intValue();
      }
    }
  }

  private static class KeyGenerator implements Runnable {

    private final HBaseLockingKeyService keyService;
    private final UUID datasetKey;
    private final int genCount;

    private KeyGenerator(HBaseLockingKeyService keyService, UUID datasetKey, int genCount) {
      this.keyService = keyService;
      this.datasetKey = datasetKey;
      this.genCount = genCount;
    }

    @Override
    public void run() {
      for (int i = 0; i < genCount; i++) {
        keyService.generateKey(ImmutableSet.of(String.valueOf(i)), datasetKey.toString());
        keysGenerated.incrementAndGet();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException {
    int hbasePoolSize = 100;
    int persistingThreads = 100;
    if (args.length == 2) {
      hbasePoolSize = Integer.valueOf(args[0]);
      persistingThreads = Integer.valueOf(args[1]);
    }
    System.out
        .println(
            "Running test with hbasePool [" + hbasePoolSize + "] and persistingThreads [" + persistingThreads + "]");
    HBaseLockingKeyServiceThroughputTest instance = new HBaseLockingKeyServiceThroughputTest(hbasePoolSize);
    instance.testNoContention(persistingThreads);
  }
}
