package org.gbif.pipelines.parsers.ws;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.config.WsConfigFactory;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;

/** Base class for tests that need a {@link MockWebServer}. */
public abstract class BaseMockServerTest {

  // mock match responses
  private static final String MATCH_RESPONSES_FOLDER = "match-responses/";
  protected static final String PUMA_CONCOLOR_RESPONSE = MATCH_RESPONSES_FOLDER + "puma-concolor.json";
  protected static final String PUMA_CONCOLOR_2_RESPONSE = MATCH_RESPONSES_FOLDER + "puma-concolor2.json";
  protected static final String PUMA_CONCOLOR_3_RESPONSE = MATCH_RESPONSES_FOLDER + "puma-concolor3.json";
  protected static final String OENANTHE_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe.json";
  protected static final String OENANTHE_2_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe2.json";
  protected static final String OENANTHE_3_RESPONSE = MATCH_RESPONSES_FOLDER + "oenanthe3.json";
  protected static final String ANNELIDA_RESPONSE = MATCH_RESPONSES_FOLDER + "annelida.json";
  protected static final String CERATIACEAE_RESPONSE = MATCH_RESPONSES_FOLDER + "ceratiaceae.json";
  protected static final String AGALLISUS_LEPTUROIDES_RESPONSE = MATCH_RESPONSES_FOLDER + "agallisus-lepturoides.json";

  // geocode test constants
  protected static final Double LATITUDE_CANADA = 60.4;
  protected static final Double LONGITUDE_CANADA = -131.3;

  private static WsConfig wsConfig;
  private static KeyValueTestStore kvStore = new KeyValueTestStore();

  /**
   * Public field because {@link ClassRule} requires it.
   *
   * <p>It uses a random port.
   */
  @ClassRule
  public static final MockWebServer MOCK_SERVER = new MockWebServer();

  @ClassRule
  public static final ExternalResource CONFIG_RESOURCE =
      new ExternalResource() {

        @Override
        protected void before() {
          wsConfig = WsConfigFactory.create(MOCK_SERVER.url("/").toString());

          kvStore.put(new LatLng(60.4d, -131.3d), Country.CANADA.getIso2LetterCode());
          kvStore.put(new LatLng(30.2d, 100.2344349d), Country.CHINA.getIso2LetterCode());
          kvStore.put(new LatLng(30.2d, 100.234435d), Country.CHINA.getIso2LetterCode());
          kvStore.put(new LatLng(71.7d, -42.6d), Country.GREENLAND.getIso2LetterCode());
          kvStore.put(new LatLng(-17.65, -149.46), Country.FRENCH_POLYNESIA.getIso2LetterCode());
          kvStore.put(new LatLng(27.15, -13.20), Country.MOROCCO.getIso2LetterCode());
        }
      };

  protected WsConfig getWsConfig() {
    return wsConfig;
  }

  protected KeyValueStore<LatLng, String> getkvStore() {
    return kvStore;
  }

  protected static void enqueueResponse(String fileName) throws IOException {
    InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
    BufferedSource source = Okio.buffer(Okio.source(inputStream));
    MockResponse mockResponse = new MockResponse();
    MOCK_SERVER.enqueue(mockResponse.setBody(source.readString(StandardCharsets.UTF_8)));
  }

  public static class KeyValueTestStore implements KeyValueStore<LatLng, String>, Serializable {

    private final Map<LatLng, String> map = new HashMap<>();

    @Override
    public String get(LatLng latLng) {
      return map.get(latLng);
    }

    @Override
    public void close() {
    }

    public void put(LatLng latLng, String value) {
      map.put(latLng, value);
    }
  }
}
