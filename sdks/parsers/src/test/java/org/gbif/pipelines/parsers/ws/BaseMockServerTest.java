package org.gbif.pipelines.parsers.ws;

import org.gbif.pipelines.parsers.ws.config.WsConfig;
import org.gbif.pipelines.parsers.ws.config.WsConfigFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

/** Base class for tests that need a {@link MockWebServer}. */
public abstract class BaseMockServerTest {

  // mock match responses
  protected static final String MATCH_RESPONSES_FOLDER = "match-responses/";
  protected static final String PUMA_CONCOLOR_RESPONSE =
      MATCH_RESPONSES_FOLDER + "puma-concolor.json";
  protected static final String PUMA_CONCOLOR_2_RESPONSE =
      MATCH_RESPONSES_FOLDER + "puma-concolor2.json";
  protected static final String PUMA_CONCOLOR_3_RESPONSE =
      MATCH_RESPONSES_FOLDER + "puma-concolor3.json";
  protected static final String OENANTHE_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe.json";
  protected static final String OENANTHE_2_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe2.json";
  protected static final String OENANTHE_3_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe3.json";
  protected static final String ANNELIDA_RESPONSE = MATCH_RESPONSES_FOLDER + "annelida.json";
  protected static final String CERATIACEAE_RESPONSE = MATCH_RESPONSES_FOLDER + "ceratiaceae.json";
  protected static final String AGALLISUS_LEPTUROIDES_RESPONSE =
      MATCH_RESPONSES_FOLDER + "agallisus-lepturoides.json";

  // mock geocode responses
  protected static final String GEOCODE_RESPONSES_FOLDER = "geocode-responses/";
  protected static final String CANADA_REVERSE_RESPONSE =
      GEOCODE_RESPONSES_FOLDER + "reverse-canada.json";
  protected static final String RUSSIA_REVERSE_RESPONSE =
      GEOCODE_RESPONSES_FOLDER + "reverse-russia.json";
  protected static final String ANTARCTICA_REVERSE_RESPONSE =
      GEOCODE_RESPONSES_FOLDER + "reverse-antarctica.json";
  protected static final String MOROCCO_WESTERN_SAHARA_REVERSE_RESPONSE =
      GEOCODE_RESPONSES_FOLDER + "reverse-morocco-western-sahara.json";
  protected static final String FRENCH_POLYNESIA_REVERSE_RESPONSE =
      GEOCODE_RESPONSES_FOLDER + "reverse-french-polynesia.json";
  protected static final String GREENLAND_REVERSE_RESPONSE =
      GEOCODE_RESPONSES_FOLDER + "reverse-greenland.json";
  protected static final String CHINA_REVERSE_RESPONSE =
      GEOCODE_RESPONSES_FOLDER + "reverse-china.json";

  // mock internal responses
  protected static final String INTERNAL_RESPONSES_FOLDER = "internal-responses/";

  // geocode test constants
  protected static final Double LATITUDE_CANADA = 60.4;
  protected static final Double LONGITUDE_CANADA = -131.3;

  private static WsConfig wsConfig;

  /**
   * Public field because {@link ClassRule} requires it.
   *
   * <p>It uses a random port.
   */
  @ClassRule public static final MockWebServer MOCK_SERVER = new MockWebServer();

  @ClassRule
  public static final ExternalResource CONFIG_RESOURCE =
      new ExternalResource() {

        @Override
        protected void before() {
          wsConfig = WsConfigFactory.create(MOCK_SERVER.url("/").toString());
        }
      };

  protected WsConfig getWsConfig() {
    return wsConfig;
  }

  protected static void enqueueResponse(String fileName) throws IOException {
    InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
    BufferedSource source = Okio.buffer(Okio.source(inputStream));
    MockResponse mockResponse = new MockResponse();
    MOCK_SERVER.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
  }

  protected static void enqueueErrorResponse(int httpCode) {
    MOCK_SERVER.enqueue(new MockResponse().setResponseCode(httpCode));
  }

  protected static void enqueueEmptyResponse() {
    MOCK_SERVER.enqueue(new MockResponse().setBody("[ ]"));
  }
}
