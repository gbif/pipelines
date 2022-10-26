package org.gbif.pipelines.core.ws.metadata;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.retry.Retry;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.xml.ws.WebServiceException;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Network;
import org.gbif.api.model.registry.Organization;
import org.gbif.pipelines.core.config.model.ContentConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.factory.RetryFactory;
import org.gbif.pipelines.core.ws.metadata.contentful.ContentService;
import org.gbif.pipelines.core.ws.metadata.contentful.ContentServiceFactory;
import org.gbif.pipelines.core.ws.metadata.response.Project;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.registry.ws.client.OrganizationClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

/** rest client for getting gbif internal api responses */
public class MetadataServiceClient {

  private final ContentService contentService;
  private final Retry retry;
  private final DatasetClient datasetClient;
  private final OrganizationClient organizationClient;
  private final InstallationClient installationClient;

  @VisibleForTesting
  protected MetadataServiceClient() {
    this.contentService = null;
    this.retry = null;
    this.datasetClient = null;
    this.organizationClient = null;
    this.installationClient = null;
  }

  private MetadataServiceClient(WsConfig wsConfig, ContentConfig contentConfig) {
    this.retry = RetryFactory.create(wsConfig.getRetryConfig(), "RegistryApiCall");
    this.contentService =
        Optional.ofNullable(contentConfig)
            .map(x -> ContentServiceFactory.getInstance(x.getEsHosts()).getService())
            .orElse(null);

    ClientBuilder clientBuilder =
        new ClientBuilder()
            .withUrl(wsConfig.getWsUrl())
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .withExponentialBackoffRetry(
                Duration.ofMillis(wsConfig.getRetryConfig().getInitialIntervalMillis()),
                wsConfig.getRetryConfig().getMultiplier(),
                wsConfig.getRetryConfig().getMaxAttempts());
    this.datasetClient = clientBuilder.build(DatasetClient.class);
    this.organizationClient = clientBuilder.build(OrganizationClient.class);
    this.installationClient = clientBuilder.build(InstallationClient.class);
  }

  public static MetadataServiceClient create(WsConfig wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new MetadataServiceClient(wsConfig, null);
  }

  public static MetadataServiceClient create(WsConfig wsConfig, ContentConfig contentConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new MetadataServiceClient(wsConfig, contentConfig);
  }

  /** Release ES content client */
  public void close() {
    if (contentService != null) {
      contentService.close();
    }
  }

  /**
   * requests https://api.gbif.org/v1/dataset/{datasetId}/networks
   *
   * @return array of networks for provided datasetId
   */
  public List<Network> getNetworkFromDataset(String datasetId) {
    Objects.requireNonNull(datasetId);
    return datasetClient.listNetworks(UUID.fromString(datasetId));
  }

  /**
   * requests https://api.gbif.org/v1/organization/{organisationId}
   *
   * @return organisation info for provided organisation id
   */
  public Organization getOrganization(UUID organizationId) {
    Objects.requireNonNull(organizationId);
    return organizationClient.get(organizationId);
  }

  public Installation getInstallation(UUID installationKey) {
    Objects.requireNonNull(installationKey);
    return installationClient.get(installationKey);
  }

  /**
   * requests http://api.gbif.org/v1/dataset/{datasetId}
   *
   * @return dataset info for provided datasetId
   */
  public Dataset getDataset(String datasetId) {
    Objects.requireNonNull(datasetId);
    return datasetClient.get(UUID.fromString(datasetId));
  }

  /** Gets Contentful data. */
  public Optional<Project> getDatasetProject(Dataset dataset) {
    if (Objects.nonNull(dataset.getProject())
        && Objects.nonNull(dataset.getProject().getIdentifier())) {
      return Optional.ofNullable(
          Retry.decorateFunction(
                  retry,
                  (Dataset d) -> {
                    try {
                      return contentService.getProject(d.getProject().getIdentifier());
                    } catch (Exception e) {
                      throw new WebServiceException(
                          "Error getting content data for dataset " + d, e);
                    }
                  })
              .apply(dataset));
    }
    return Optional.empty();
  }
}
