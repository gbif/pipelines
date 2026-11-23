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
package org.gbif.pipelines.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.orchestrator.OrchestratorReplies;
import org.gbif.pipelines.spark.IndexingPipeline;
import org.gbif.pipelines.spark.ValidatorMetricsPipeline;

@Slf4j
public class ValidatorMetricsCallback
    extends ValidatorCallback<PipelinesInterpretedMessage, PipelinesIndexedMessage>
    implements MessageCallback<PipelinesInterpretedMessage> {

  public ValidatorMetricsCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VALIDATOR_COLLECT_METRICS;
  }

  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {}

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    log.info("Run metrics for validation: {}", message.getDatasetUuid());
    ValidatorMetricsPipeline.run(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        OrchestratorReplies.header(message).isPresent());
    log.info("Finished metrics for validation: {}", message.getDatasetUuid());
  }

  /** The metrics the job wrote, reported as {@code INTERPRETATION_METRICS}. */
  @Override
  protected OrchestratorResult orchestratorResult(PipelinesInterpretedMessage message)
      throws IOException {
    Path path =
        new Path(
            String.format(
                "%s/%s/%d/%s",
                pipelinesConfig.getOutputPath(),
                message.getDatasetUuid(),
                message.getAttempt(),
                ValidatorMetricsPipeline.METRICS_FILENAME));
    JsonNode metrics;
    try (InputStream in = fileSystem.open(path)) {
      metrics = OrchestratorJson.mapper().readTree(in);
    }
    return new OrchestratorResult(
        Map.of(), List.of(new Report(ReportTypes.INTERPRETATION_METRICS, metrics, null)));
  }

  @Override
  protected String getMetaFileName() {
    return IndexingPipeline.METRICS_FILENAME;
  }

  @Override
  public Class<PipelinesInterpretedMessage> getMessageClass() {
    return PipelinesInterpretedMessage.class;
  }

  @Override
  public PipelinesIndexedMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return null;
  }
}
