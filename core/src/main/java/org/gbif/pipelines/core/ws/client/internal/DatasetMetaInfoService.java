package org.gbif.pipelines.core.ws.client.internal;

import com.google.gson.JsonElement;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

/**
 * retro client for gbif services.
 */
interface DatasetMetaInfoService {

  /**
   * get networks info of provided dataset uuid.
   * @param UUID
   * @return JsonElement with networks info for provided dataset uuid.
   */
  @GET("dataset/{datasetUUID}/networks")
  Call<JsonElement> getNetworks(@Path("datasetUUID") String UUID);

  /**
   * get dataset info of provided dataset uuid.
   * @param UUID
   * @return JsonElement with provided dataset info.
   */
  @GET("dataset/{datasetUUID}")
  Call<JsonElement> getDataset(@Path("datasetUUID") String UUID);

  /**
   * get installation info of provided installation uuid.
   * @param UUID
   * @return JsonElement with provided installation info.
   */
  @GET("installation/{installationUUID}")
  Call<JsonElement> getInstallation(@Path("installationUUID") String UUID);

  /**
   * get organization info of provided organization uuid.
   * @param UUID
   * @return JsonElement with organization info
   */
  @GET("organization/{organizationUUID}")
  Call<JsonElement> getOrganization(@Path("organizationUUID") String UUID);

}
