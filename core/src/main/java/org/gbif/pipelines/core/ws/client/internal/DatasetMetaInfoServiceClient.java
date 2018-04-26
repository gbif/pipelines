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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
public class DatasetMetaInfoServiceClient {

  private static final String INSTALLATION_KEY = "installationKey";
  private static final String PUB_ORGANIZATION_KEY = "publishingOrganizationKey";
  private static final String DATASET_TITLE_KEY = "title";
  private static final String PUB_ORG_COUNTRY_KEY = "country";
  private static final String INSTALLATION_TYPE_KEY = "type";
  private static final String DATASET_NETWORK_KEY = "key";
  private final DatasetMetaInfoService service;

  /**
   * create client from provided configuration
   */
  public static DatasetMetaInfoServiceClient from(Config wsConfig) {
    // create client
    OkHttpClient client = HttpClientFactory.createClient(wsConfig);

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .validateEagerly(true)
      .build();

    return new DatasetMetaInfoServiceClient(retrofit.create(DatasetMetaInfoService.class));
  }

  /**
   * initialize Client with default values
   */
  public static DatasetMetaInfoServiceClient client() {
    return DatasetMetaInfoServiceClient.from(HttpConfigFactory.createConfig(Service.DATASET_META));
  }

  private DatasetMetaInfoServiceClient(DatasetMetaInfoService internalService) { this.service = internalService;}

  /**
   * perform webservice call when needed to aggregate the needed GBIF terms
   *
   * @param datasetUUID datasetUUID
   *
   * @return aggregated GBIFTerms response for the provided datasetUUID
   */
  public DatasetMetaInfoResponse getDatasetMetaInfo(String datasetUUID) throws ExecutionException {
    Objects.requireNonNull(datasetUUID,"DatasetUUID cannot be null");
    DatasetMetaInfoResponse.DatasetMetaInfoResponseBuilder responseBuilder = DatasetMetaInfoResponse.newBuilder().using(datasetUUID);


    JsonObject dataset = getDataset(datasetUUID);

    Optional.ofNullable(dataset.getAsJsonPrimitive(DATASET_TITLE_KEY))
      .ifPresent(title -> responseBuilder.addDatasetTitle(title.getAsString()));

    Optional.ofNullable(dataset.getAsJsonPrimitive(PUB_ORGANIZATION_KEY)).ifPresent((orgKey) -> {
      JsonObject orgInfo = getOrganization(orgKey.getAsString());
      responseBuilder.addPublishingOrgKey(orgKey.getAsString());
      Optional.ofNullable(orgInfo.getAsJsonPrimitive(PUB_ORG_COUNTRY_KEY))
        .ifPresent(countryObject -> responseBuilder.addPublishingCountry(countryObject.getAsString()));
    });

    Optional.ofNullable(dataset.getAsJsonPrimitive(INSTALLATION_KEY)).ifPresent((key) -> {
      JsonObject installationInfo = getInstallation(key.getAsString());
      Optional.ofNullable(installationInfo.getAsJsonPrimitive(INSTALLATION_TYPE_KEY))
        .ifPresent(type -> responseBuilder.addProtocol(type.getAsString()));
    });

    JsonArray networks = getNetworkFromDataset(datasetUUID);
    List<String> networkKeys = new ArrayList<>(networks.size());
    networks.iterator()
      .forEachRemaining(element -> networkKeys.add(element.getAsJsonObject().get(DATASET_NETWORK_KEY).getAsString()));
    responseBuilder.addNetworkKey(networkKeys);
    return responseBuilder.build();
  }

  /**
   * requests https://api.gbif.org/v1/dataset/{datasetid}/networks
   *
   * @return array of networks for provided datasetID
   */
  public JsonArray getNetworkFromDataset(String datasetUUID) {
    Objects.requireNonNull(datasetUUID);
    return performCall(service.getNetworks(datasetUUID)).getAsJsonArray();
  }

  /**
   * requests https://api.gbif.org/v1/organisation/{organisationid}
   *
   * @return organisation info for provided organisation id
   */
  public JsonObject getOrganization(String organizationUUID) {
    Objects.requireNonNull(organizationUUID);
    return performCall(service.getOrganization(organizationUUID)).getAsJsonObject();
  }

  /**
   * requests http://api.gbif.org/v1/dataset/{datasetid}
   *
   * @return dataset info for provided dataset id
   */
  public JsonObject getDataset(String datasetUUID) {
    Objects.requireNonNull(datasetUUID);
    return performCall(service.getDataset(datasetUUID)).getAsJsonObject();
  }

  /**
   * requests http://api.gbif.org/v1/installation/{installation_id}
   *
   * @return installation info
   */
  public JsonObject getInstallation(String installationUUID) {
    Objects.requireNonNull(installationUUID);
    return performCall(service.getInstallation(installationUUID)).getAsJsonObject();
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
