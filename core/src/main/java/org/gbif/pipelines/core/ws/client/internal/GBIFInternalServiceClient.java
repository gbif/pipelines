package org.gbif.pipelines.core.ws.client.internal;

import org.gbif.pipelines.core.ws.HttpClientFactory;
import org.gbif.pipelines.core.ws.HttpConfigFactory;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/**
 * rest client for getting gbif internal api responses
 */
public class GBIFInternalServiceClient {

  private static final String INSTALLATION_KEY = "installationKey";
  private static final String PUB_ORGANIZATION_KEY = "publishingOrganizationKey";
  private static final String DATASET_TITLE_KEY = "title";
  private static final String PUB_ORG_COUNTRY_KEY = "country";
  private static final String INSTALLATION_TYPE_KEY = "type";
  private static final String DATASET_NETWORK_KEY = "key";
  private final GBIFInternalService service;
  //caches the last requests obtained
  private final Cache<String, GBIFInternalResponse> datasetResponseCache =
    CacheBuilder.newBuilder().maximumSize(10000).build();

  /**
   * create client from provided configuration
   */
  public static GBIFInternalServiceClient from(Config wsConfig) {
    // create client
    OkHttpClient client = HttpClientFactory.createClient(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    return new GBIFInternalServiceClient(retrofit.create(GBIFInternalService.class));
  }

  /**
   * initialize Client with default values
   */
  public static GBIFInternalServiceClient client() {
    return GBIFInternalServiceClient.from(HttpConfigFactory.createConfig(Service.GBIF_INTERNAL));
  }

  private GBIFInternalServiceClient(GBIFInternalService internalService) { this.service = internalService;}

  /**
   * requests https://api.gbif.org/v1/dataset/{datasetid}/networks
   *
   * @return array of networks for provided datasetID
   */
  public JsonArray getNetworkFromDataset(String datasetUUID) {
    Objects.requireNonNull(datasetUUID);
    return performCall(service.getNetworkFromDataset(datasetUUID)).getAsJsonArray();
  }

  /**
   * requests https://api.gbif.org/v1/organisation/{organisationid}
   *
   * @return organisation info for provided organisation id
   */
  public JsonObject getOrganizationInfo(String organizationUUID) {
    Objects.requireNonNull(organizationUUID);
    return performCall(service.getOrganizationInfo(organizationUUID)).getAsJsonObject();
  }

  /**
   * requests http://api.gbif.org/v1/dataset/{datasetid}
   *
   * @return dataset info for provided dataset id
   */
  public JsonObject getDatasetInfo(String datasetUUID) {
    Objects.requireNonNull(datasetUUID);
    return performCall(service.getDatasetInfo(datasetUUID)).getAsJsonObject();
  }

  /**
   * requests http://api.gbif.org/v1/installation/{installation_id}
   *
   * @return installation info
   */
  public JsonObject getInstallationInfo(String installationUUID) {
    Objects.requireNonNull(installationUUID);
    return performCall(service.getInstallationInfo(installationUUID)).getAsJsonObject();
  }

  /**
   * perform webservice call to aggregate the needed GBIF terms
   *
   * @param datasetUUID datasetUUID
   *
   * @return aggregated GBIFTerms response for the provided datasetUUID
   */
  public GBIFInternalResponse getInternalResponse(String datasetUUID) throws ExecutionException {
    Objects.requireNonNull(datasetUUID, "DatasetUUID cannot be null");
    //create callable on cache miss the GBIFInternalResponse for the dataset id is stored in the cache
    Callable<GBIFInternalResponse> callableResponse = () -> {
      GBIFInternalResponse response = new GBIFInternalResponse();
      response.setDatasetKey(datasetUUID);

      JsonObject dataset = getDatasetInfo(datasetUUID);

      Optional.ofNullable(dataset.getAsJsonPrimitive(DATASET_TITLE_KEY))
        .ifPresent((title) -> response.setDatasetTitle(title.getAsString()));

      Optional.ofNullable(dataset.getAsJsonPrimitive(PUB_ORGANIZATION_KEY)).ifPresent((orgKey) -> {
        JsonObject orgInfo = getOrganizationInfo(orgKey.getAsString());
        response.setPublishingOrgKey(orgKey.getAsString());
        Optional.ofNullable(orgInfo.getAsJsonPrimitive(PUB_ORG_COUNTRY_KEY))
          .ifPresent((countryObject) -> response.setPublishingCountry(countryObject.getAsString()));
      });

      Optional.ofNullable(dataset.getAsJsonPrimitive(INSTALLATION_KEY)).ifPresent((key) -> {
        JsonObject installationInfo = getInstallationInfo(key.getAsString());
        Optional.ofNullable(installationInfo.getAsJsonPrimitive(INSTALLATION_TYPE_KEY))
          .ifPresent((type) -> response.setProtocol(type.getAsString()));
      });

      JsonArray networks = getNetworkFromDataset(datasetUUID);
      List<String> networkKeys = new ArrayList<>(networks.size());
      networks.iterator()
        .forEachRemaining((element) -> networkKeys.add(element.getAsJsonObject()
                                                         .get(DATASET_NETWORK_KEY)
                                                         .getAsString()));
      response.setNetworkKey(networkKeys);
      return response;
    };
    return datasetResponseCache.get(datasetUUID, callableResponse);
  }

  /**
   * executes request and handles response and errors.
   */
  private JsonElement performCall(Call<JsonElement> serviceCall) {
    try {
      Response<JsonElement> execute = serviceCall.execute();
      if (execute.isSuccessful()) {
        return execute.body();
      } else {
        throw new RuntimeException("Request "
                                   + serviceCall.request()
                                   + " failed with status code "
                                   + execute.code()
                                   + " and error response "
                                   + (Objects.nonNull(execute.errorBody()) ? execute.errorBody().string() : null));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error making request " + serviceCall.request(), e);
    }
  }

}
