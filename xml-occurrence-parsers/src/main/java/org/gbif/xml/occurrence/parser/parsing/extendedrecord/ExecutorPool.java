package org.gbif.xml.occurrence.parser.parsing.extendedrecord;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Singleton class to create only one ForkJoinPool instance
 */
public class ExecutorPool {

  private final Executor pool;

  private ExecutorPool(int parallelism) {
    pool = new ForkJoinPool(parallelism);
  }

  private static class LazyHolder {

    private static ExecutorPool instance;
  }

  public static ExecutorPool getInstance(int parallelism) {
    if (Objects.isNull(LazyHolder.instance)) {
      LazyHolder.instance = new ExecutorPool(parallelism);
    }
    return LazyHolder.instance;
  }

  public Executor getPool() {
    return pool;
  }
}