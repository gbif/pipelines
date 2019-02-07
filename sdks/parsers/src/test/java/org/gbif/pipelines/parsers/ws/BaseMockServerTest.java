package org.gbif.pipelines.parsers.ws;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeKVStoreConfiguration;
import org.gbif.kvs.geocode.GeocodeKVStoreFactory;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.kvs.hbase.HBaseKVStoreConfiguration;
import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.config.WsConfigFactory;
import org.gbif.rest.client.configuration.ClientConfiguration;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.BufferedSource;
import okio.Okio;

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

  private static KeyValueStore<LatLng, String> kvStore;

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
        protected void before() throws IOException{
          wsConfig = WsConfigFactory.create(MOCK_SERVER.url("/").toString());

          kvStore =
              GeocodeKVStoreFactory.simpleGeocodeKVStore(GeocodeKVStoreConfiguration.builder()
                      .withJsonColumnQualifier("j") //stores JSON data
                      .withCountryCodeColumnQualifier("c") //stores ISO country code
                      .withHBaseKVStoreConfiguration(HBaseKVStoreConfiguration.builder()
                          .withTableName("geocode_kv") //Geocode KV HBase table
                          .withColumnFamily("v") //Column in which qualifiers are stored
                          .withNumOfKeyBuckets(10) //Buckets for salted key generations == to # of region servers
                          .withHBaseZk("c5zk1.gbif.org,c5zk2.gbif.org,c5zk3.gbif.org") //HBase Zookeeper ensemble
                          .build()).build(),
                  ClientConfiguration.builder()
                      .withBaseApiUrl("https://api.gbif.org/v1/") //GBIF base API url
                      .withFileCacheMaxSizeMb(64L) //Max file cache size
                      .withTimeOut(60L) //Geocode service connection time-out
                      .build());
        }
      };

  protected WsConfig getWsConfig() {
    return wsConfig;
  }

  protected KeyValueStore<LatLng, String> getkvStore() {
    return kvStore;
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
