package org.gbif.pipelines.core.ws.client.internal;

import org.gbif.pipelines.core.ws.client.internal.response.Dataset;
import org.gbif.pipelines.core.ws.client.internal.response.Installation;
import org.gbif.pipelines.core.ws.client.internal.response.Network;
import org.gbif.pipelines.core.ws.client.internal.response.Organization;
import org.gbif.pipelines.core.ws.config.Config;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import retrofit2.Call;
import retrofit2.Response;

/** rest client for getting gbif internal api responses */
public class MetadataServiceClient {

  private final MetadataServiceRest rest;

  private MetadataServiceClient(Config wsConfig) {
    rest = MetadataServiceRest.getInstance(wsConfig);
  }

  public static MetadataServiceClient create(Config wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new MetadataServiceClient(wsConfig);
  }

  /**
   * requests https://api.gbif.org/v1/dataset/{datasetid}/networks
   *
   * @return array of networks for provided datasetID
   */
  public List<Network> getNetworkFromDataset(String datasetId) {
    Objects.requireNonNull(datasetId);
    return performCall(rest.getService().getNetworks(datasetId));
  }

  /**
   * requests https://api.gbif.org/v1/organisation/{organisationid}
   *
   * @return organisation info for provided organisation id
   */
  public Organization getOrganization(String organizationId) {
    Objects.requireNonNull(organizationId);
    return performCall(rest.getService().getOrganization(organizationId));
  }

  /**
   * requests http://api.gbif.org/v1/dataset/{datasetid}
   *
   * @return dataset info for provided dataset id
   */
  public Dataset getDataset(String datasetId) {
    Objects.requireNonNull(datasetId);
    return performCall(rest.getService().getDataset(datasetId));
  }

  /**
   * requests http://api.gbif.org/v1/installation/{installation_id}
   *
   * @return installation info
   */
  public Installation getInstallation(String installationId) {
    Objects.requireNonNull(installationId);
    return performCall(rest.getService().getInstallation(installationId));
  }

  /** executes request and handles response and errors. */
  private <T> T performCall(Call<T> serviceCall) {
    try {
      Response<T> execute = serviceCall.execute();
      if (execute.isSuccessful()) {
        return execute.body();
      } else {
        throw new IllegalArgumentException(
            "Request "
                + serviceCall.request()
                + " failed with status code "
                + execute.code()
                + " and error response "
                + (Objects.nonNull(execute.errorBody()) ? execute.errorBody().string() : null));
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Error making request " + serviceCall.request(), e);
    }
  }
}
