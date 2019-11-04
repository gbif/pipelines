package org.gbif.pipelines.parsers.ws.client.metadata;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.gbif.pipelines.parsers.config.RetryFactory;
import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Network;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Organization;

import javax.xml.ws.WebServiceException;

import io.github.resilience4j.retry.Retry;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

/** rest client for getting gbif internal api responses */
public class MetadataServiceClient {

  private final MetadataServiceRest rest;
  private final Retry retry;

  private MetadataServiceClient(WsConfig wsConfig) {
    rest = MetadataServiceRest.getInstance(wsConfig);
    retry = RetryFactory.create(wsConfig.getRetryConfig(), "RegistryApiCall");
  }

  public static MetadataServiceClient create(WsConfig wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new MetadataServiceClient(wsConfig);
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
    return performCall(call);
  }

  /** executes request and handles response and errors. */
  private <T> T performCall(Call<T> serviceCall) {
    return
      Retry.decorateFunction(retry, (Call<T> call) -> {
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
                                                   }
      ).apply(serviceCall);
  }

}
