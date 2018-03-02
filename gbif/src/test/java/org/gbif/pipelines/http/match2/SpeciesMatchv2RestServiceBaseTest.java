package org.gbif.pipelines.http.match2;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.http.MockServerTest;
import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.http.HttpResponse;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.net.HttpURLConnection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.mockwebserver.MockResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;

public class SpeciesMatchv2RestServiceTest extends MockServerTest {

  @BeforeClass
  public static void setUp() throws IOException {
    mockServerSetUp();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    mockServerTearDown();
  }

  @Test
  public void simpleCallTest() throws IOException {
    SpeciesMatchv2Service service = SpeciesMatchv2Rest.getInstance(configMockServer).getService();

    enqueueResponse(PUMA_CONCOLOR_RESPONSE);

    final String name = "Puma concolor";

    Call<NameUsageMatch2> call = service.match(null, null, null, null, null, null, null, name, true, false);

    try {
      Response<NameUsageMatch2> response = call.execute();

      Gson gson = new GsonBuilder().setPrettyPrinting().create();

      Assert.assertNotNull(response);

      System.out.println(gson.toJson(response.body()));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void shouldReturn500error() {
    SpeciesMatchv2Service service = SpeciesMatchv2Rest.getInstance(configMockServer).getService();

    SpeciesMatchv2Client client = new SpeciesMatchv2Client(service);

    mockServer.enqueue(new MockResponse().setResponseCode(HttpURLConnection.HTTP_INTERNAL_ERROR));

    ExtendedRecord record = new ExtendedRecordCustomBuilder().name("Puma concolor").id("1").build();

    HttpResponse<NameUsageMatch2> response = client.getMatch(record);

    Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getHttpResponseCode().intValue());
    Assert.assertEquals(HttpResponse.ErrorCode.CALL_FAILED, response.getErrorCode());
  }

}
