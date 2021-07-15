package org.gbif.validator.ws.it;

import static org.gbif.validator.ws.it.ValidatorWsItConfiguration.TEST_USER;
import static org.gbif.validator.ws.it.ValidatorWsItConfiguration.TEST_USER_PASSWORD;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import lombok.SneakyThrows;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.XmlSchemaValidatorResult;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.HttpStatusCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.SocketUtils;

/** Base class for IT tests that initializes data sources and basic security settings. */
@ExtendWith(SpringExtension.class)
@ExtendWith(MockServerExtension.class)
@SpringBootTest(
    classes = ValidatorWsItConfiguration.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = {ValidationResourceIT.ContextInitializerItTests.class})
@ActiveProfiles("test")
@AutoConfigureMockMvc
public class ValidationResourceIT {

  // Directory used as temporary space to upload files
  @TempDir static Path workingDirectory;

  // Directory used as files store
  @TempDir static Path storeDirectory;

  private static ClientAndServer clientAndServer;

  private final ValidationWsClient validationWsClient;

  @Autowired
  public ValidationResourceIT(@LocalServerPort int port) {
    validationWsClient =
        new ClientBuilder()
            .withUrl("http://localhost:" + port)
            .withCredentials(TEST_USER.getUserName(), TEST_USER_PASSWORD)
            .withFormEncoder()
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .build(ValidationWsClient.class);
  }

  @BeforeAll
  public static void init() {
    clientAndServer = ClientAndServer.startClientAndServer(SocketUtils.findAvailableTcpPort());
    setExpectations();
  }

  /** Set expected responses for the ClientAndServer mock server. */
  private static void setExpectations() {
    clientAndServer
        .when(HttpRequest.request().withMethod("GET").withPath("/Archive.zip"))
        .respond(HttpResponse.response().withBody(readTestFile("/Archive.zip")));

    // HEAD requests are used to check if a file is available
    clientAndServer
        .when(HttpRequest.request().withMethod("HEAD").withPath("/Archive.zip"))
        .respond(HttpResponse.response().withStatusCode(HttpStatusCode.OK_200.code()));
  }

  /** Creates a path to the a local MockServer url. */
  protected String testPath(String path) {
    return "http://127.0.0.1:" + clientAndServer.getPort() + path;
  }

  @SneakyThrows
  protected static byte[] readTestFile(String file) {
    return Files.readAllBytes(Paths.get(ValidationResourceIT.class.getResource(file).getFile()));
  }

  @AfterAll
  public static void tearDown() {
    clientAndServer.stop();
  }

  @Test
  public void validationListIT() {
    PagingResponse<Validation> validations = validationWsClient.list(new PagingRequest(0, 10));
    Assertions.assertNotNull(validations);
  }

  @Test
  public void validationSubmitFileIT() {
    File archive = readTestFileInputStream("/Archive.zip");
    Validation validation = validationWsClient.submitFile(archive);
    Assertions.assertNotNull(validation);

    // Can the new validation be retrieved?
    Validation persistedValidation = validationWsClient.get(validation.getKey());
    Assertions.assertNotNull(persistedValidation);
  }

  @Test
  public void validationSubmitUrlIT() {
    Validation validation = validationWsClient.submitUrl(testPath("/Archive.zip"));
    Assertions.assertNotNull(validation);
  }

  @Test
  public void validationUpdateIT() {
    File archive = readTestFileInputStream("/Archive.zip");
    Validation validation = validationWsClient.submitFile(archive);

    validation.setStatus(Validation.Status.FINISHED);

    Metrics metrics =
        Metrics.builder()
            .core(Metrics.Core.builder().indexedCount(100L).build())
            .extensions(
                Collections.singletonList(
                    Metrics.Extension.builder().rowType("occurrence").fileCount(1L).build()))
            .archiveValidationReport(
                Metrics.ArchiveValidationReport.builder()
                    .occurrenceReport(
                        new OccurrenceValidationReport(
                            100, 100,
                            0, 100,
                            0, true))
                    .build())
            .xmlSchemaValidatorResult(
                XmlSchemaValidatorResult.builder().errors(Collections.EMPTY_LIST).build())
            .build();
    validation.setMetrics(metrics);
    validationWsClient.update(validation);
    Validation persistedValidation = validationWsClient.get(validation.getKey());
    Assertions.assertEquals(metrics, persistedValidation.getMetrics());
    Assertions.assertEquals(Validation.Status.FINISHED, persistedValidation.getStatus());
  }

  @SneakyThrows
  protected static File readTestFileInputStream(String file) {
    return new File(ValidationResourceIT.class.getResource(file).getFile());
  }

  static class ContextInitializerItTests
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {

      TestPropertyValues.of(testPropertyPairs()).applyTo(configurableApplicationContext);
    }

    /** Creates the registry datasource settings from the embedded database. */
    private static String[] testPropertyPairs() {
      return new String[] {
        "upload.workingDirectory=" + workingDirectory.toString(),
        "upload.maxUploadSize=3145728",
        "storePath=" + storeDirectory.toString(),
        "messaging.enabled=false"
      };
    }
  }
}
