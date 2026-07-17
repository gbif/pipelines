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
package org.gbif.pipelines.tasks.tracking;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.gbif.api.model.pipelines.PipelineStep;

/**
 * Shared pipeline step status groups used by callback tracking and queueing logic.
 *
 * <p>The history service stores each pipeline step with a status. Several callback components need
 * to answer the same questions consistently:
 *
 * <ul>
 *   <li>Has this step already been processed or claimed?
 *   <li>Is this step in a terminal state?
 *   <li>Can this submitted step still be moved to queued?
 * </ul>
 *
 * <p>This utility class centralizes those status groups so the meaning of "processed" and
 * "finished" stays consistent across the callback implementation.
 */
public final class PipelinesStepSets {

  /**
   * Statuses that indicate a step has already been claimed, processed, or terminated.
   *
   * <p>Steps in these states should not be queued again as newly submitted work.
   */
  static final Set<PipelineStep.Status> PROCESSED =
      new HashSet<>(
          Arrays.asList(
              PipelineStep.Status.RUNNING,
              PipelineStep.Status.FAILED,
              PipelineStep.Status.COMPLETED,
              PipelineStep.Status.ABORTED));

  /**
   * Terminal statuses for a pipeline step.
   *
   * <p>Once a step is in one of these states, callback tracking should avoid overwriting it with a
   * later status update.
   */
  static final Set<PipelineStep.Status> FINISHED =
      new HashSet<>(
          Arrays.asList(
              PipelineStep.Status.COMPLETED,
              PipelineStep.Status.ABORTED,
              PipelineStep.Status.FAILED));

  private PipelinesStepSets() {}

  /** Returns {@code true} when the step has already been claimed, processed, or terminated. */
  public static boolean isProcessedStep(PipelineStep.Status status) {
    return PROCESSED.contains(status);
  }

  /** Returns {@code true} when the step has already been claimed, processed, or terminated. */
  public static boolean isProcessedStep(PipelineStep step) {
    return isProcessedStep(step.getState());
  }

  /** Returns {@code true} when the step is still eligible for first-time queue processing. */
  public static boolean isNotProcessedStep(PipelineStep.Status status) {
    return !isProcessedStep(status);
  }

  /** Returns {@code true} when the step is in a terminal state. */
  public static boolean isFinishedStep(PipelineStep.Status status) {
    return FINISHED.contains(status);
  }
}
