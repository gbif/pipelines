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
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class ValidatorIndexingDistributedCallback extends ValidatorIndexingCallback {

  public ValidatorIndexingDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher, null);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    // TODO: implement
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
