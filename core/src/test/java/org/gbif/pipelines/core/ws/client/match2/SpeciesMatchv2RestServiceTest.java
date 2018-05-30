package org.gbif.pipelines.core.ws.client.match2;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.MockServer;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.net.HttpURLConnection;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;

public class SpeciesMatchv2RestServiceTest extends MockServer {

  @Test
  public void simpleCallTest() throws IOException {
    SpeciesMatchv2Service service = SpeciesMatchv2ServiceRest.getInstance().getService();

    enqueueResponse(PUMA_CONCOLOR_RESPONSE);

    final String name = "Puma concolor";

    Call<NameUsageMatch2> call = service.match(null, null, null, null, null, null, null, name, true, false);

    try {
      Response<NameUsageMatch2> response = call.execute();

      Assert.assertNotNull(response);

      ObjectMapper mapper = new ObjectMapper();
      System.out.println(mapper.writer(new DefaultPrettyPrinter()).writeValueAsString(response.body()));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void shouldReturn500error() {
    mockServer.enqueue(new MockResponse().setResponseCode(HttpURLConnection.HTTP_INTERNAL_ERROR));

    ExtendedRecord record = ExtendedRecordCustomBuilder.create().name("Puma concolor").id("1").build();

    HttpResponse<NameUsageMatch2> response = SpeciesMatchv2Client.newInstance().getMatch(record);

    Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.getHttpResponseCode().intValue());
    Assert.assertEquals(HttpResponse.ErrorCode.CALL_FAILED, response.getErrorCode());
  }

}
