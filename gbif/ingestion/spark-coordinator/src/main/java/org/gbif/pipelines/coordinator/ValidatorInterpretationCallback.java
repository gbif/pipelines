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

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.OccurrenceInterpretationPipeline;

@Slf4j
public class ValidatorInterpretationCallback
    extends PipelinesCallback<PipelinesVerbatimMessage, PipelinesInterpretedMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public ValidatorInterpretationCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    OccurrenceInterpretationPipeline.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VERBATIM_TO_INTERPRETED;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {
    // TODO: implement
  }

  @Override
  protected String getMetaFileName() {
    return OccurrenceInterpretationPipeline.METRICS_FILENAME;
  }

  public PipelinesInterpretedMessage createOutgoingMessage(PipelinesVerbatimMessage message) {
    // TODO: implement
    return null;
  }

  @Override
  public Class<PipelinesVerbatimMessage> getMessageClass() {
    return PipelinesVerbatimMessage.class;
  }
}