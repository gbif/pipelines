package org.gbif.pipelines.tasks.dwcdp;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

// import java.time.Duration;
// import lombok.extern.slf4j.Slf4j;
// import org.gbif.api.service.registry.DatasetDataPackageService;
// import org.gbif.common.messaging.MessageListener;
// import org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage;
// import org.gbif.pipelines.common.configs.RegistryConfiguration;
// import org.gbif.pipelines.common.configs.StepConfiguration;
// import org.gbif.registry.ws.client.DatasetDataPackageClient;
// import org.gbif.ws.client.ClientBuilder;
// import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage } and initiates an Airflow
 * DAG.
 */
@Slf4j
public abstract class DwcDpService extends AbstractIdleService {
  //
  //  private final DwcDpConfiguration config;
  //  private MessageListener listener;
  //
  //  public DwcDpService(DwcDpConfiguration config) {
  //    this.config = config;
  //  }
  //
  //  @Override
  //  protected void startUp() throws Exception {
  //    log.info("Started pipelines-dwc-dp service");
  //    // Prefetch is one, since this is a long-running process.
  //    StepConfiguration c = config.stepConfig;
  //    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
  //
  //    DatasetDataPackageService datasetDataPackageService =
  //        newClientBuilder(config.stepConfig.registry).build(DatasetDataPackageClient.class);
  //    DwcDpCallback callback =
  //        DwcDpCallback.builder()
  //            .config(config)
  //            .datasetDataPackageService(datasetDataPackageService)
  //            .build();
  //
  //    listener.listen(c.queueName, DwcDpDownloadFinishedMessage.ROUTING_KEY, c.poolSize,
  // callback);
  //  }
  //
  //  /**
  //   * Convenience method to provide a ws client factory. The factory will be used to create
  // writable
  //   * registry clients.
  //   *
  //   * @return writable client factory
  //   */
  //  public ClientBuilder newClientBuilder(RegistryConfiguration registryConfiguration) {
  //    // setup writable registry client
  //    return new ClientBuilder()
  //        .withUrl(registryConfiguration.wsUrl)
  //        .withCredentials(registryConfiguration.user, registryConfiguration.password)
  //        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
  //        // This will give up to 40 tries, from 2 to 75 seconds apart, over at most 13 minutes
  //        // (approx)
  //        .withExponentialBackoffRetry(Duration.ofSeconds(2), 1.1, 40);
  //  }
  //
  //  @Override
  //  protected void shutDown() {
  //    listener.close();
  //    log.info("Stopping pipelines-dwc-dp service");
  //  }
}
