package org.gbif.pipelines.spark;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.net.URL;
import java.nio.file.Files;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

public abstract class MockedServicesTest {

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  private static WireMockServer registryServer;
  private static WireMockServer nameMatchingServer;
  private static WireMockServer grsciCollServer;
  private static WireMockServer geocodeServer;

  // downstream-visible ports
  protected static int registryPort;
  protected static int nameMatchingPort;
  protected static int grsciCollPort;
  protected static int geocodePort;

  @ClassRule
  public static ExternalResource wireMockResource =
      new ExternalResource() {
        @Override
        protected void before() throws Exception {

          registryServer = new WireMockServer(wireMockConfig().dynamicPort().dynamicHttpsPort());
          nameMatchingServer =
              new WireMockServer(wireMockConfig().dynamicPort().dynamicHttpsPort());
          grsciCollServer = new WireMockServer(wireMockConfig().dynamicPort().dynamicHttpsPort());
          geocodeServer = new WireMockServer(wireMockConfig().dynamicPort().dynamicHttpsPort());

          registryServer.start();
          nameMatchingServer.start();
          grsciCollServer.start();
          geocodeServer.start();

          // capture ports AFTER start()
          registryPort = registryServer.port();
          nameMatchingPort = nameMatchingServer.port();
          grsciCollPort = grsciCollServer.port();
          geocodePort = geocodeServer.port();

          URL testRootUrl = EventInterpretationTest.class.getResource("/");
          assert testRootUrl != null;
          String testResourcesRoot = testRootUrl.getFile();

          // setup hbase, registry, geocode, grscicoll, taxon web services mocks here
          nameMatchingServer.stubFor(
              get(anyUrl())
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody(
                              Files.readString(
                                  java.nio.file.Path.of(
                                      testResourcesRoot + "ws/namematching-response.json")))));

          geocodeServer.stubFor(
              get(anyUrl())
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody(
                              Files.readString(
                                  java.nio.file.Path.of(
                                      testResourcesRoot + "ws/geocode-response.json")))));

          grsciCollServer.stubFor(
              get(anyUrl())
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody(
                              Files.readString(
                                  java.nio.file.Path.of(
                                      testResourcesRoot + "ws/grscioll-response.json")))));
          String uuid =
              "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

          registryServer.stubFor(
              get(urlPathMatching("/v1/dataset/" + uuid))
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody(
                              Files.readString(
                                  java.nio.file.Path.of(
                                      testResourcesRoot + "ws/registry-response.json")))));

          registryServer.stubFor(
              get(urlPathMatching("/v1/dataset/" + uuid + "/networks"))
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody("[]")));

          registryServer.stubFor(
              get(urlPathMatching("/v1/organization/" + uuid))
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody(
                              Files.readString(
                                  java.nio.file.Path.of(
                                      testResourcesRoot + "ws/organization-response.json")))));

          registryServer.stubFor(
              get(urlPathMatching("/v1/installation/" + uuid))
                  .willReturn(
                      aResponse()
                          .withStatus(200)
                          .withHeader("Content-Type", "application/json")
                          .withBody(
                              Files.readString(
                                  java.nio.file.Path.of(
                                      testResourcesRoot + "ws/installation-response.json")))));

          TestConfigUtil.createConfigYaml(
              testResourcesRoot,
              testResourcesRoot,
              HBASE_SERVER.getZkQuorum(),
              registryPort,
              nameMatchingPort,
              geocodePort,
              grsciCollPort,
              "pipelines.yaml");
        }

        @Override
        protected void after() {
          // optional: don't stop to allow JVM reuse
          // wireMockRule.stop();
        }
      };
}
