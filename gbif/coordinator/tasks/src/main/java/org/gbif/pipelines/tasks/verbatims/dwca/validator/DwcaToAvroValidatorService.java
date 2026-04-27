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
package org.gbif.pipelines.tasks.verbatims.dwca.validator;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesValidatorDwcaMessage } and perform conversion
 */
@Slf4j
public class DwcaToAvroValidatorService extends AbstractIdleService {

  private final DwcaToAvroValidatorConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public DwcaToAvroValidatorService(DwcaToAvroValidatorConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-validator-verbatim-to-avro-from-dwca service");
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    ValidationWsClient validationClient =
        ServiceFactory.createValidationWsClient(config.stepConfig);

    DatasetClient datasetClient = ServiceFactory.createDatasetClient(config.stepConfig);

    DwcaToAvroValidatorCallback validatorCallback =
        DwcaToAvroValidatorCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    listener.listen(c.queueName, validatorCallback.getRouting(), c.poolSize, validatorCallback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    log.info("Stopping pipelines-validator-verbatim-to-avro-from-dwca service");
  }
}
