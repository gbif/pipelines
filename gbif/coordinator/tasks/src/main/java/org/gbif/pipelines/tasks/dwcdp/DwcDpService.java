package org.gbif.pipelines.tasks.dwcdp;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.service.registry.DatasetDataPackageService;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.ws.client.ClientBuilder;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage } and initiates a Airflow
 * DAG.
 */
@Slf4j
public class DwcDpService extends AbstractIdleService {

  private final DwcDpConfiguration config;
  private MessageListener listener;

  public DwcDpService(DwcDpConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-dwc-dp service");
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);

    DatasetDataPackageService datasetDataPackageService = config.registryConfig.newClientBuilder().build(DatasetDataPackageService.class);
    DwcDpCallback callback = DwcDpCallback.builder().config(config).datasetDataPackageService(datasetDataPackageService).build();

    listener.listen(c.queueName, DwcDpDownloadFinishedMessage.ROUTING_KEY, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    log.info("Stopping pipelines-dwc-dp service");
  }
}
