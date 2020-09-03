package org.gbif.pipelines.core.ws.metadata;

import java.util.List;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.metadata.response.Network;
import org.gbif.pipelines.core.ws.metadata.response.Organization;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

/** retro client for gbif services. */
interface MetadataService {

  /**
   * get networks info of provided dataset uuid.
   *
   * @param datasetId datasetId
   * @return JsonElement with networks info for provided dataset uuid.
   */
  @GET("/v1/dataset/{datasetId}/networks")
  Call<List<Network>> getNetworks(@Path("datasetId") String datasetId);

  /**
   * get dataset info of provided dataset uuid.
   *
   * @param datasetId datasetId
   * @return JsonElement with provided dataset info.
   */
  @GET("/v1/dataset/{datasetId}")
  Call<Dataset> getDataset(@Path("datasetId") String datasetId);

  /**
   * get organization info of provided organization uuid.
   *
   * @param organizationId organizationId
   * @return JsonElement with organization info
   */
  @GET("/v1/organization/{organizationId}")
  Call<Organization> getOrganization(@Path("organizationId") String organizationId);
}
