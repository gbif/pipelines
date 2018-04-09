package org.gbif.pipelines.core.ws;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tests that need a {@link MockWebServer}.
 */
public abstract class MockServer {

  private static final Logger LOG = LoggerFactory.getLogger(MockServer.class);

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
  protected static final String DUMMY_RESPONSE = MATCH_RESPONSES_FOLDER + "dummy-response.json";

  // mock geocode responses
  protected static final String GEOCODE_RESPONSES_FOLDER = "geocode-responses/";
  protected static final String CANADA_REVERSE_RESPONSE = GEOCODE_RESPONSES_FOLDER + "reverse-canada.json";
  protected static final String RUSSIA_REVERSE_RESPONSE = GEOCODE_RESPONSES_FOLDER + "reverse-russia.json";
  protected static final String ANTARCTICA_REVERSE_RESPONSE = GEOCODE_RESPONSES_FOLDER + "reverse-antarctica.json";
  protected static final String MOROCCO_WESTERN_SAHARA_REVERSE_RESPONSE =
    GEOCODE_RESPONSES_FOLDER + "reverse-morocco-western-sahara.json";
  protected static final String FRENCH_POLYNESIA_REVERSE_RESPONSE =
    GEOCODE_RESPONSES_FOLDER + "reverse-french-polynesia.json";
  protected static final String GREENLAND_REVERSE_RESPONSE = GEOCODE_RESPONSES_FOLDER + "reverse-greenland.json";
  protected static final String CHINA_REVERSE_RESPONSE = GEOCODE_RESPONSES_FOLDER + "reverse-china.json";

  // geocode test constants
  protected static final Double LATITUDE_CANADA = 60.4;
  protected static final Double LONGITUDE_CANADA = -131.3;

  protected static MockWebServer mockServer;

  @ClassRule
  public static ExternalResource serverResource = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      mockServer = new MockWebServer();
      // TODO: check if the port is in use??
      mockServer.start(1111);
    }

    @Override
    protected void after() {
      try {
        mockServer.shutdown();
      } catch (IOException e) {
        LOG.error("Could not stop the mock web server", e);
      }
    }
  };

  protected static void enqueueResponse(String fileName) throws IOException {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
    BufferedSource source = Okio.buffer(Okio.source(inputStream));
    MockResponse mockResponse = new MockResponse();
    mockServer.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
  }

  protected static void enqueueErrorResponse(int httpCode) {
    mockServer.enqueue(new MockResponse().setResponseCode(httpCode));
  }

  protected static void enqueueEmptyResponse() {
    mockServer.enqueue(new MockResponse().setBody("[ ]"));
  }
}
