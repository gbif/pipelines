/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.tasks;

import com.fasterxml.jackson.core.JsonParseException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/**
 * Guards regular pipeline processing against conflicting or stopped executions.
 *
 * <p>The history service stores the currently running execution for a dataset. This class compares
 * that running execution with the execution id carried by the incoming message and decides whether
 * the message should continue processing.
 *
 * <p>The guard prevents two important cases:
 *
 * <ul>
 *   <li>starting a new execution while another execution is already running for the same dataset
 *   <li>continuing an old or aborted execution after the running execution has changed or
 *       disappeared
 * </ul>
 *
 * <p>Messages without an execution id are treated as requests to start a new execution. Messages
 * with an execution id are treated as part of an existing execution and must match the running
 * execution stored by the history service.
 */
@Slf4j
@RequiredArgsConstructor
public class PipelinesExecutionGuard {

  /**
   * Retry used when checking the running execution for an existing pipeline execution.
   *
   * <p>For messages that already have an execution id, the running execution key is expected to
   * exist unless the execution was aborted or stopped. The history service may be temporarily
   * unavailable or may briefly return {@code null}, so the call is retried before deciding that
   * processing should stop.
   */
  private static final Retry RUNNING_EXECUTION_CALL =
      Retry.of(
          "runningExecutionCall",
          RetryConfig.custom()
              .maxAttempts(15)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .retryOnResult(Objects::isNull)
              .build());

  private final PipelinesHistoryClient historyClient;

  /**
   * Returns {@code true} when the incoming message should not continue processing.
   *
   * <p>The decision is based on two values:
   *
   * <ul>
   *   <li>the execution id in the incoming message
   *   <li>the currently running execution id stored by the history service for the dataset
   * </ul>
   *
   * <p>The rules are:
   *
   * <ul>
   *   <li>If both ids are absent, this is a new execution and processing may continue.
   *   <li>If the message has no execution id but the history service has a running execution, a
   *       different execution is already running and processing must stop.
   *   <li>If the message has an execution id but the history service has no running execution, the
   *       execution was likely aborted or stopped and processing must stop.
   *   <li>If both ids exist but differ, the message belongs to an old or different execution and
   *       processing must stop.
   * </ul>
   */
  public boolean isProcessingStopped(PipelineBasedMessage message) {
    Long currentKey = message.getExecutionId();
    UUID datasetUuid = message.getDatasetUuid();

    Long runningKey =
        currentKey == null
            ? getRunningExecutionKey(datasetUuid)
            : getRunningExecutionKeyWithRetry(datasetUuid);

    if (currentKey == null && runningKey == null) {
      log.info("Continue execution. New execution and no other running executions");
      return false;
    }
    if (currentKey == null) {
      log.warn("Can't run new execution if some other execution is running");
      return true;
    }
    if (runningKey == null) {
      log.warn("Stop execution. Execution is aborted");
      return true;
    }

    return !currentKey.equals(runningKey);
  }

  /** Returns the current running execution key for the dataset, or {@code null} if none exists. */
  private Long getRunningExecutionKey(UUID datasetUuid) {
    return historyClient.getRunningExecutionKey(datasetUuid);
  }

  /**
   * Returns the current running execution key, retrying temporary failures and transient {@code
   * null} results.
   *
   * <p>This is used only for messages that already have an execution id. In that case the running
   * execution should normally be visible in the history service. A final {@code null} result means
   * the execution is no longer running and should be treated as stopped.
   */
  private Long getRunningExecutionKeyWithRetry(UUID datasetUuid) {
    Supplier<Long> supplier = () -> historyClient.getRunningExecutionKey(datasetUuid);
    return RUNNING_EXECUTION_CALL.executeSupplier(supplier);
  }
}
