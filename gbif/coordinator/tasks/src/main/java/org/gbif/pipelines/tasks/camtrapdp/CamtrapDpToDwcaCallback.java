package org.gbif.pipelines.tasks.camtrapdp;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcaDownloadFinishedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesCamtrapDpMessage;
import org.gbif.registry.ws.client.DatasetClient;

/** Callback which is called when the {@link PipelinesCamtrapDpMessage} is received. */
@Slf4j
@AllArgsConstructor
public class CamtrapDpToDwcaCallback extends AbstractMessageCallback<PipelinesCamtrapDpMessage> {

  private final CamtrapToDwcaConfiguration config;
  private final MessagePublisher publisher;
  private final DatasetClient datasetClient;

  @Override
  public void handleMessage(PipelinesCamtrapDpMessage message) {
    toDwca(message);
    notifyNextStep(message);
  }

  /** Calls the Camtraptor Service to transform the data package into DwC-A. */
  private void toDwca(PipelinesCamtrapDpMessage message) {
    Dataset dataset = datasetClient.get(message.getDatasetUuid());
    CamtraptorWsClient camtraptorWsClient = new CamtraptorWsClient(config.camtraptorWsUrl);
    camtraptorWsClient.toDwca(dataset.getKey(), dataset.getTitle());
  }

  /** Creates and sends a message to the next step: DwC-A validation. */
  @SneakyThrows
  private void notifyNextStep(PipelinesCamtrapDpMessage message) {
    publisher.send(
        new PipelinesBalancerMessage(
            PipelinesArchiveValidatorMessage.class.getSimpleName(),
            createOutgoingMessage(message).toString()));
  }

  /** Builds the message to be sent to the next processing stage: DwC-A validation. */
  private DwcaDownloadFinishedMessage createOutgoingMessage(PipelinesCamtrapDpMessage message) {
    return new DwcaDownloadFinishedMessage(
        message.getDatasetUuid(),
        message.getSource(),
        message.getAttempt(),
        new Date(),
        true,
        EndpointType.DWC_ARCHIVE,
        message.getPlatform());
  }

  public String getRouting() {
    return PipelinesCamtrapDpMessage.ROUTING_KEY;
  }
}
