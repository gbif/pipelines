package org.gbif.pipelines;

import org.gbif.pipelines.http.config.Config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;

/**
 * Base class for tests that need a {@link MockWebServer}.
 */
public abstract class MockServerTest {

  // mock match responses
  protected static final String MATCH_RESPONSES_FOLDER = "match-responses/";
  protected static final String PUMA_CONCOLOR_RESPONSE = MATCH_RESPONSES_FOLDER + "puma-concolor.json";
  protected static final String PUMA_CONCOLOR_2_RESPONSE = MATCH_RESPONSES_FOLDER + "puma-concolor2.json";
  protected static final String PUMA_CONCOLOR_3_RESPONSE = MATCH_RESPONSES_FOLDER + "puma-concolor3.json";
  protected static final String OENANTHE_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe.json";
  protected static final String OENANTHE_2_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe2.json";
  protected static final String OENANTHE_3_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe3.json";
  protected static final String ANNELIDA_RESPONSE = MATCH_RESPONSES_FOLDER + "annelida.json";
  protected static final String CERATIACEAE_RESPONSE = MATCH_RESPONSES_FOLDER + "ceratiaceae.json";
  protected static final String AGALLISUS_LEPTUROIDES_RESPONSE = MATCH_RESPONSES_FOLDER + "agallisus-lepturoides.json";

  // mock geocode responses
  protected static final String GEOCODE_RESPONSES_FOLDER = "geocode-responses/";
  protected static final String REVERSE_CANADA_RESPONSE = GEOCODE_RESPONSES_FOLDER + "reverse-canada.json";

  protected static MockWebServer mockServer;
  protected static Config configMockServer;

  protected static void mockServerSetUp() throws IOException {
    mockServer = new MockWebServer();

    // config to use the mock server
    configMockServer = new Config();
    configMockServer.setBasePath(mockServer.url("/").toString());
  }

  protected static void mockServerTearDown() throws IOException {
    mockServer.shutdown();
  }

  protected static void enqueueResponse(String fileName) throws IOException {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
    BufferedSource source = Okio.buffer(Okio.source(inputStream));
    MockResponse mockResponse = new MockResponse();
    mockServer.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
  }

  protected static void enqueueErrorResponse(int httpCode) {
    mockServer.enqueue(new MockResponse().setResponseCode(httpCode));
  }

}
