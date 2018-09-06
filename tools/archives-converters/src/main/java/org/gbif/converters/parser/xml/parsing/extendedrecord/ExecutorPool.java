package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/** Singleton class to create only one Executor instance */
public class ExecutorPool {

  private static volatile Executor instance;
  private static final Object MUTEX = new Object();

  private ExecutorPool() {}

  public static Executor getInstance(int parallelism) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = Executors.newFixedThreadPool(parallelism);
        }
      }
    }
    return instance;
  }
}
