package org.gbif.pipelines.ingest.java.pipelines.interpretation;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;

/** Closes resources only one time, before JVM shuts down */
@Slf4j
public class Shutdown {

  private static volatile Shutdown instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private Shutdown(
      BasicTransform bTr, LocationTransform lTr, GrscicollTransform gTr, GbifIdTransform idTr) {
    Runnable shudownHook =
        () -> {
          log.info("Closing all resources");
          bTr.tearDown();
          lTr.tearDown();
          gTr.tearDown();
          idTr.tearDown();
          log.info("The resources were closed");
        };
    Runtime.getRuntime().addShutdownHook(new Thread(shudownHook));
  }

  public static void doOnExit(
      BasicTransform bTr, LocationTransform lTr, GrscicollTransform gTr, GbifIdTransform idTr) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new Shutdown(bTr, lTr, gTr, idTr);
        }
      }
    }
  }
}
