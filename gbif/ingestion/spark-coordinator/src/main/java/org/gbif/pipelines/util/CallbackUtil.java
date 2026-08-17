package org.gbif.pipelines.util;

import java.io.File;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CallbackUtil {

  public static final String PAUSE_FILE_PATH = "/tmp/pause_message_processing";
  public static final String SHUTDOWN_FILE_PATH = "/tmp/shutdown_now";
  public static final String SIMULATE_BACKEND_FAIL_PATH = "/tmp/backend_fail";
  public static final String SIMULATE_PIPELINES_STEP_FAIL_PATH = "/tmp/pipeline_step_fail";

  public static void checkIfPaused() {
    while (new File(PAUSE_FILE_PATH).exists()) {
      log.warn(
          "Found "
              + PAUSE_FILE_PATH
              + " file, pausing processing new messages for 30s. Delete to resume.");
      try {
        Thread.sleep(30_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  public static boolean simulateBackendFail() {
    boolean enabled = new File(SIMULATE_BACKEND_FAIL_PATH).exists();
    if (enabled) {
      log.warn("Simulating backend failure ({} exists)", SIMULATE_BACKEND_FAIL_PATH);
    }
    return enabled;
  }

  public static boolean simulatePipelineStepFail() {
    boolean enabled = new File(SIMULATE_PIPELINES_STEP_FAIL_PATH).exists();
    if (enabled) {
      log.warn("Simulating pipeline step failure ({} exists)", SIMULATE_PIPELINES_STEP_FAIL_PATH);
    }
    return enabled;
  }

  public static boolean isRunning() {
    return !new File(SHUTDOWN_FILE_PATH).exists();
  }
}
