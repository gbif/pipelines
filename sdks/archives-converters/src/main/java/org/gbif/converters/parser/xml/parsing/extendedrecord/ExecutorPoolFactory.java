package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Singleton class to create only one Executor instance */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExecutorPoolFactory {

  private static volatile Executor instance;
  private static final Object MUTEX = new Object();

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
