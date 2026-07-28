package org.gbif.pipelines.util;

import java.io.File;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CallbackUtil {

  public static final String PAUSE_FILE_PATH = "/tmp/pause_message_processing";
  public static final String SHUTDOWN_FILE_PATH = "/tmp/shutdown_now";

  public static void checkIfPaused() {
    while (new File(PAUSE_FILE_PATH).exists()) {
      log.warn(
          "Found "
              + PAUSE_FILE_PATH
              + " file, pausing processing new messages for 10s. Delete to resume.");
      try {
        Thread.sleep(30_000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static boolean isRunning() {
    return !new File(SHUTDOWN_FILE_PATH).exists();
  }
}
