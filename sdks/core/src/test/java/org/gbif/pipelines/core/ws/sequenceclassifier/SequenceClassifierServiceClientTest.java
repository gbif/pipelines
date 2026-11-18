package org.gbif.pipelines.core.ws.sequenceclassifier;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import java.util.List;
import javax.xml.ws.WebServiceException;
import org.gbif.pipelines.core.config.model.RetryConfig;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import retrofit2.HttpException;

class SequenceClassifierServiceClientTest {

  private static final String PATH = "/occurrence/classify/batch";

  // one server for all the tests: the service factory is a singleton bound to the first url
  private static WireMockServer server;
  private static SequenceClassifierServiceClient client;

  private static final List<ClassifyOccurrenceRequest> REQUESTS =
      List.of(request("1"), request("2"));

  @BeforeAll
  static void setUp() {
    server = new WireMockServer(wireMockConfig().dynamicPort());
    server.start();

    RetryConfig retryConfig = new RetryConfig();
    retryConfig.setMaxAttempts(3);
    retryConfig.setInitialIntervalMillis(10L);
    WsConfig wsConfig = new WsConfig();
    wsConfig.setWsUrl(server.baseUrl());
    wsConfig.setRetryConfig(retryConfig);
    client = SequenceClassifierServiceClient.create(wsConfig);
  }

  @AfterAll
  static void tearDown() {
    server.stop();
  }

  @BeforeEach
  void reset() {
    server.resetAll();
  }

  private static ClassifyOccurrenceRequest request(String gbifID) {
    ClassifyOccurrenceRequest request = new ClassifyOccurrenceRequest();
    request.setGbifID(gbifID);
    return request;
  }

  private static ResponseDefinitionBuilder json(int status, String body) {
    return aResponse()
        .withStatus(status)
        .withHeader("Content-Type", "application/json")
        .withBody(body);
  }

  private static void respond(int status, String body) {
    server.stubFor(post(urlPathEqualTo(PATH)).willReturn(json(status, body)));
  }

  /** Fails once with the given status, then succeeds. */
  private static void failOnceThenSucceed(int status) {
    server.stubFor(
        post(urlPathEqualTo(PATH))
            .inScenario("transient error")
            .whenScenarioStateIs(STARTED)
            .willReturn(json(status, "{}"))
            .willSetStateTo("recovered"));
    server.stubFor(
        post(urlPathEqualTo(PATH))
            .inScenario("transient error")
            .whenScenarioStateIs("recovered")
            .willReturn(json(200, "[{\"gbifID\": \"1\"}, {\"gbifID\": \"2\"}]")));
  }

  @Test
  void retriesRetryableErrorsThenSucceeds() {
    for (int status : new int[] {429, 502, 503, 504}) {
      server.resetAll();
      failOnceThenSucceed(status);

      List<ClassifyOccurrenceResponse> responses = client.classifyBatch(REQUESTS);

      assertEquals(2, responses.size());
      assertEquals("1", responses.get(0).getGbifID());
      assertEquals("2", responses.get(1).getGbifID());
      server.verify(2, postRequestedFor(urlPathEqualTo(PATH)));
    }
  }

  @Test
  void failsAfterRetryingRetryableErrors() {
    respond(503, "{}");

    assertThrows(WebServiceException.class, () -> client.classifyBatch(REQUESTS));
    server.verify(3, postRequestedFor(urlPathEqualTo(PATH)));
  }

  @Test
  void internalServerErrorIsNotRetried() {
    respond(500, "{}");

    assertThrows(HttpException.class, () -> client.classifyBatch(REQUESTS));
    server.verify(1, postRequestedFor(urlPathEqualTo(PATH)));
  }

  @Test
  void clientErrorIsNotRetried() {
    respond(400, "{}");

    assertThrows(HttpException.class, () -> client.classifyBatch(REQUESTS));
    server.verify(1, postRequestedFor(urlPathEqualTo(PATH)));
  }

  @Test
  void nullBodyReturnsNoResults() {
    respond(200, "null");

    assertTrue(client.classifyBatch(REQUESTS).isEmpty());
  }

  @Test
  void nullResultsAreDropped() {
    respond(200, "[{\"gbifID\": \"1\"}, null]");

    List<ClassifyOccurrenceResponse> responses = client.classifyBatch(REQUESTS);

    assertEquals(1, responses.size());
    assertEquals("1", responses.get(0).getGbifID());
  }

  /** The occurrences without a result are handled by the caller. */
  @Test
  void partialResponseIsReturned() {
    respond(200, "[{\"gbifID\": \"2\"}]");

    List<ClassifyOccurrenceResponse> responses = client.classifyBatch(REQUESTS);

    assertEquals(1, responses.size());
    assertEquals("2", responses.get(0).getGbifID());
  }
}
