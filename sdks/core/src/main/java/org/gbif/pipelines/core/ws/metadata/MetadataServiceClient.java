package org.gbif.pipelines.core.ws.metadata;

import io.github.resilience4j.retry.Retry;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.xml.ws.WebServiceException;
import org.gbif.pipelines.core.config.factory.RetryFactory;
import org.gbif.pipelines.core.config.model.ContentConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.ws.metadata.contentful.ContentService;
import org.gbif.pipelines.core.ws.metadata.contentful.ContentServiceFactory;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.metadata.response.Network;
import org.gbif.pipelines.core.ws.metadata.response.Organization;
import org.gbif.pipelines.core.ws.metadata.response.Project;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

/** rest client for getting gbif internal api responses */
public class MetadataServiceClient {

  private final MetadataServiceFactory rest;
  private final ContentService contentService;
  private final Retry retry;

  private MetadataServiceClient(WsConfig wsConfig, ContentConfig contentConfig) {
    rest = MetadataServiceFactory.getInstance(wsConfig);
    retry = RetryFactory.create(wsConfig.getRetryConfig(), "RegistryApiCall");
    contentService =
        Optional.ofNullable(contentConfig)
            .map(x -> ContentServiceFactory.getInstance(x.getEsHosts()).getService())
            .orElse(null);
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
    Call<List<Network>> call = rest.getService().getNetworks(datasetId);
    return performCall(call);
  }

  /**
   * requests https://api.gbif.org/v1/organisation/{organisationId}
   *
   * @return organisation info for provided organisation id
   */
  public Organization getOrganization(String organizationId) {
    Objects.requireNonNull(organizationId);
    Call<Organization> call = rest.getService().getOrganization(organizationId);
    return performCall(call);
  }

  /**
   * requests http://api.gbif.org/v1/dataset/{datasetId}
   *
   * @return dataset info for provided datasetId
   */
  public Dataset getDataset(String datasetId) {
    Objects.requireNonNull(datasetId);
    Call<Dataset> call = rest.getService().getDataset(datasetId);
    Dataset dataset = performCall(call);
    // Has Contenful Elastic being configured?
    if (Objects.nonNull(contentService)) {
      getContentProjectData(dataset).ifPresent(dataset::setProject);
    }
    return dataset;
  }

  /** Gets Contentful data. */
  private Optional<Project> getContentProjectData(Dataset dataset) {
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

  /** executes request and handles response and errors. */
  private <T> T performCall(Call<T> serviceCall) {
    return Retry.decorateFunction(
            retry,
            (Call<T> call) -> {
              try {
                Response<T> execute = call.execute();
                if (execute.isSuccessful()) {
                  return execute.body();
                } else {
                  throw new HttpException(execute);
                }
              } catch (IOException e) {
                throw new WebServiceException("Error making request " + call.request(), e);
              }
            })
        .apply(serviceCall);
  }
}
