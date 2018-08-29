package org.gbif.pipelines.core.ws.client.match2;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.core.ws.BaseMockServerTest;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.net.HttpURLConnection;

import okhttp3.mockwebserver.MockResponse;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;

public class SpeciesMatchv2RestServiceTest extends BaseMockServerTest {

  @Test
  public void simpleCallTest() throws IOException {

    // State
    SpeciesMatchv2Service service =
        SpeciesMatchv2ServiceRest.getInstance(getWsConfig()).getService();
    enqueueResponse(PUMA_CONCOLOR_RESPONSE);

    final String name = "Puma concolor";

    // When
    Call<NameUsageMatch2> call =
        service.match(null, null, null, null, null, null, null, name, true, false);
    Response<NameUsageMatch2> response = call.execute();

    // Should
    Assert.assertNotNull(response);
  }

  @Test
  public void shouldReturn500error() {

    // State
    mockServer.enqueue(new MockResponse().setResponseCode(HttpURLConnection.HTTP_INTERNAL_ERROR));

    ExtendedRecord record =
        ExtendedRecordCustomBuilder.create().name("Puma concolor").id("1").build();

    // When
    HttpResponse<NameUsageMatch2> response =
        SpeciesMatchv2Client.create(getWsConfig()).getMatch(record);

    // Should
    Assert.assertEquals(
        HttpURLConnection.HTTP_INTERNAL_ERROR, response.getHttpResponseCode().intValue());
    Assert.assertEquals(HttpResponse.ErrorCode.CALL_FAILED, response.getErrorCode());
  }
}
