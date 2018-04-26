package org.gbif.pipelines.core.ws.client.internal;

import com.google.gson.JsonElement;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

/**
 * retro client for gbif services.
 */
interface DatasetMetaInfoService {

  @GET("dataset/{datasetUUID}/networks")
  Call<JsonElement> getNetworkFromDataset(@Path("datasetUUID") String UUID);

  @GET("dataset/{datasetUUID}")
  Call<JsonElement> getDatasetInfo(@Path("datasetUUID") String UUID);

  @GET("installation/{installationUUID}")
  Call<JsonElement> getInstallationInfo(@Path("installationUUID") String UUID);

  @GET("organization/{organizationUUID}")
  Call<JsonElement> getOrganizationInfo(@Path("organizationUUID") String UUID);

}
