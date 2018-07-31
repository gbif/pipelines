package org.gbif.pipelines.core.ws.client.internal;

import org.gbif.pipelines.core.ws.MockServer;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import okio.BufferedSource;
import okio.Okio;
import org.apache.commons.compress.utils.Charsets;
import org.junit.Assert;
import org.junit.Test;

public class DatasetMetaInfoServiceClientTest extends MockServer {

  private static final String DATASET_KEY = "key";
  private static final String PUBLISHING_COUNTRY_KEY = "country";
  private static final String PROTOCOL_KEY = "type";
  private static final String PUBLISH_ORG_KEY = "publishingOrganizationKey";
  private static final String NETWORK_KEY = "key";
  private static final String DATASET_TITLE_KEY = "title";

  private static final String INVALID_DATASET_KEY_VAL = "4fa7b334-ce0d-4e88-aaae-2e0c138d049f";

  private static final String INSTALLATION_KEY_VALUE = "7182d304-b0a2-404b-baba-2086a325c221";
  private static final String DATASET_KEY_VAL = "4fa7b334-ce0d-4e88-aaae-2e0c138d049e";
  private static final String PUBLISHING_COUNTRY_KEY_VAL = "US";
  private static final String PROTOCOL_KEY_VAL = "HTTP_INSTALLATION";
  private static final String PUBLISH_ORG_KEY_VAL = "e2e717bf-551a-4917-bdc9-4fa0f342c530";
  private static final String[] NETWORK_KEY_VAL =
    new String[] {"7ddd1f14-a2b0-4838-95b0-785846f656f3", "e8fbd857-c988-48e5-954f-6e14d0961137"};
  private static final String DATASET_TITLE_KEY_VAL = "EOD - eBird Observation Dataset";

  @Test
  public void testDatasetResponse() throws IOException {
    enqueueResponse(EOD_DATASET_INTERNAL_RESPONSE);

    JsonObject response = DatasetMetaInfoServiceClient.client().getDataset(DATASET_KEY_VAL);
    Assert.assertEquals(DATASET_KEY_VAL, response.get(DATASET_KEY).getAsString());
    Assert.assertEquals(DATASET_TITLE_KEY_VAL, response.get(DATASET_TITLE_KEY).getAsString());
    Assert.assertEquals(PUBLISH_ORG_KEY_VAL, response.get(PUBLISH_ORG_KEY).getAsString());
  }

  @Test
  public void testNetworkResponse() throws IOException {
    enqueueResponse(EOD_NETWORKS_INTERNAL_RESPONSE);

    JsonArray response = DatasetMetaInfoServiceClient.client().getNetworkFromDataset(DATASET_KEY_VAL);
    System.out.println(prettyPrint(response));
    Assert.assertEquals(2, response.size());
    Assert.assertEquals(NETWORK_KEY_VAL[0],
                        response.get(0).getAsJsonObject().getAsJsonPrimitive(NETWORK_KEY).getAsString());
    Assert.assertEquals(NETWORK_KEY_VAL[1],
                        response.get(1).getAsJsonObject().getAsJsonPrimitive(NETWORK_KEY).getAsString());
  }

  @Test
  public void testOrganizationResponse() throws IOException {
    enqueueResponse(EOD_ORGANIZATION_INTERNAL_RESPONSE);

    JsonObject response = DatasetMetaInfoServiceClient.client().getOrganization(PUBLISH_ORG_KEY_VAL);
    System.out.println(prettyPrint(response));
    Assert.assertEquals(PUBLISHING_COUNTRY_KEY_VAL, response.get(PUBLISHING_COUNTRY_KEY).getAsString());
  }

  @Test
  public void testInstallationResponse() throws IOException {
    enqueueResponse(EOD_INSTALL_INTERNAL_RESPONSE);

    JsonObject response = DatasetMetaInfoServiceClient.client().getInstallation(INSTALLATION_KEY_VALUE);
    System.out.println(prettyPrint(response));
    Assert.assertEquals(PROTOCOL_KEY_VAL, response.get(PROTOCOL_KEY).getAsString());
  }

  @Test
  public void testFinalResponse() throws ExecutionException, IOException {
    enqueueResponse(EOD_DATASET_INTERNAL_RESPONSE);
    enqueueResponse(EOD_ORGANIZATION_INTERNAL_RESPONSE);
    enqueueResponse(EOD_INSTALL_INTERNAL_RESPONSE);
    enqueueResponse(EOD_NETWORKS_INTERNAL_RESPONSE);

    Assert.assertEquals(readFile(EOD_INTERNAL_RESPONSE), validatePositive(DATASET_KEY_VAL));
  }

  @Test(expected = RuntimeException.class)
  public void invalidDatasetID() throws ExecutionException {
    enqueueErrorResponse(404);
    System.out.println(validatePositive(INVALID_DATASET_KEY_VAL));
  }

  @Test(expected = RuntimeException.class)
  public void nullDatasetID() throws ExecutionException {
    System.out.println(validatePositive(null));
  }

  String validatePositive(String datasetUUID) throws ExecutionException {
    DatasetMetaInfoResponse object = DatasetMetaInfoServiceClient.client().getDatasetMetaInfo(datasetUUID);
    return prettyPrint(object);
  }

  String prettyPrint(Object obj) {
    return new GsonBuilder().setPrettyPrinting().create().toJson(obj);
  }

  String readFile(String fileName) throws IOException {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
    BufferedSource source = Okio.buffer(Okio.source(inputStream));
    return source.readString(Charsets.UTF_8);
  }

}
