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

import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;

/**
 * Holds the tracking identifiers and step map produced when a pipeline step is registered in the
 * history service. Instances are created by {@link PipelinesStepTracker#track} and passed back to
 * the caller so it can update the same step after processing completes and queue downstream steps.
 */
@Getter
@Builder
public class TrackingInfo {
  private final long processKey;
  private final long executionId;
  private final long stepKey;
  private final String datasetId;
  private final String attempt;
  private final Map<StepType, PipelineStep> pipelineStepMap;
}
