package org.gbif.validator.ws.it;

import static org.gbif.validator.ws.it.ValidatorWsItConfiguration.TEST_USER;
import static org.gbif.validator.ws.it.ValidatorWsItConfiguration.TEST_USER_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.vocabulary.NameUsageIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.TestSocketUtils;

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
        ValidationWsClient.getInstance(
            "http://localhost:" + port, TEST_USER.getUserName(), TEST_USER_PASSWORD);
  }

  @BeforeAll
  public static void init() {
    clientAndServer = ClientAndServer.startClientAndServer(TestSocketUtils.findAvailableTcpPort());
    setExpectations();
  }

  /** Set expected responses for the ClientAndServer mock server. */
  private static void setExpectations() {
    clientAndServer
        .when(HttpRequest.request().withMethod("GET").withPath("/archive.zip"))
        .respond(HttpResponse.response().withBody(readTestFile("/archive.zip")));

    // HEAD requests are used to check if a file is available
    clientAndServer
        .when(HttpRequest.request().withMethod("HEAD").withPath("/archive.zip"))
        .respond(HttpResponse.response().withStatusCode(HttpStatusCode.OK_200.code()));
  }

  /** Creates a path to a local MockServer url. */
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
    PagingResponse<Validation> validations =
        validationWsClient.list(ValidationSearchRequest.builder().build());
    assertNotNull(validations);
  }

  private static ValidationRequest testValidationRequest() {
    return ValidationRequest.builder()
        .installationKey(UUID.randomUUID())
        .sourceId(UUID.randomUUID().toString())
        .notificationEmail(new HashSet<>(Arrays.asList("nobody@gbif.org", "test@gbif.org")))
        .build();
  }

  @Test
  public void validationSubmitFileIT() throws Exception {
    File archive = readTestFileInputStream("/archive.zip");
    ValidationRequest validationRequest = testValidationRequest();
    Validation validation = validationWsClient.validateFile(archive, validationRequest);

    // Wait for internal async task
    TimeUnit.SECONDS.sleep(3L);

    assertNotNull(validation);
    assertEquals(validationRequest.getInstallationKey(), validation.getInstallationKey());
    assertEquals(validationRequest.getSourceId(), validation.getSourceId());
    // Emails must not be exposed
    assertNull(validation.getNotificationEmails());

    // Can the new validation be retrieved?
    Validation persistedValidation = validationWsClient.get(validation.getKey());
    assertNotNull(persistedValidation);
    assertNull(persistedValidation.getDataset());

    // Wait for the submit operation to complete
    TimeUnit.SECONDS.sleep(2L);

    PagingResponse<Validation> validations =
        validationWsClient.list(
            ValidationSearchRequest.builder()
                .offset(0L)
                .limit(10)
                .status(Collections.singleton(Status.QUEUED))
                .sortByCreated(ValidationSearchRequest.SortOrder.DESC)
                .build());
    assertNotNull(validations.getCount());
    assertTrue(validations.getCount() > 0);

    PagingResponse<Validation> runningValidations =
        validationWsClient.list(
            ValidationSearchRequest.builder()
                .offset(0L)
                .limit(10)
                .status(Collections.singleton(Validation.Status.RUNNING))
                .build());
    assertEquals(0, runningValidations.getCount());

    Calendar aYearFromNow = Calendar.getInstance();
    aYearFromNow.add(Calendar.YEAR, 1);
    PagingResponse<Validation> aYearFromNowValidations =
        validationWsClient.list(
            ValidationSearchRequest.builder()
                .offset(0L)
                .limit(10)
                .fromDate(aYearFromNow.getTime())
                .build());
    assertEquals(0, aYearFromNowValidations.getCount());
  }

  @Test
  public void validationSubmitCsvFileIT() throws Exception {
    File archive = readTestFileInputStream("/file.csv");
    ValidationRequest validationRequest = testValidationRequest();
    Validation validation = validationWsClient.validateFile(archive, validationRequest);

    // Wait for internal async task
    TimeUnit.SECONDS.sleep(3L);

    assertNotNull(validation);
    assertEquals(validationRequest.getInstallationKey(), validation.getInstallationKey());
    assertEquals(validationRequest.getSourceId(), validation.getSourceId());
    // Emails must not be exposed
    assertNull(validation.getNotificationEmails());

    // Can the new validation be retrieved?
    Validation persistedValidation = validationWsClient.get(validation.getKey());
    assertNotNull(persistedValidation);
    assertNull(persistedValidation.getDataset());

    assertEquals(Status.QUEUED, persistedValidation.getStatus());
    assertFalse(persistedValidation.failed());
  }

  @Test
  @Disabled("URL is validated and checked for content type")
  public void validationSubmitUrlIT() {
    ValidationRequest validationRequest = testValidationRequest();
    Validation validation =
        validationWsClient.validateFileFromUrl(testPath("/archive.zip"), validationRequest);
    assertNotNull(validation);
    assertEquals(validationRequest.getInstallationKey(), validation.getInstallationKey());
    assertEquals(validationRequest.getSourceId(), validation.getSourceId());
    // Emails must not be exposed
    assertNull(validation.getNotificationEmails());
  }

  /** Test the notification addresses are sent when the installationKey is present. */
  @Test
  public void validationSubmitUrlMissingEmailsIT() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            validationWsClient.validateFileFromUrl(
                testPath("/archive.zip"),
                ValidationRequest.builder()
                    .sourceId(UUID.randomUUID().toString())
                    .installationKey(UUID.randomUUID())
                    .build()));
  }

  @Test
  public void validationSubmitUrlWrongEmailFormatIT() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            validationWsClient.validateFileFromUrl(
                testPath("/archive.zip"),
                ValidationRequest.builder()
                    .notificationEmail(Collections.singleton("thisnotandEmail!gmail.com"))
                    .build()));
  }

  @Test
  public void validationUpdateIT() {
    File archive = readTestFileInputStream("/archive.zip");
    Validation validation = validationWsClient.submitFile(archive);

    validation.setStatus(Validation.Status.FINISHED);

    NameUsage nameUsage = new NameUsage();
    nameUsage.setScientificName("Wild dog");
    nameUsage.setIssues(Collections.singleton(NameUsageIssue.ACCEPTED_NAME_MISSING));

    Metrics metrics =
        Metrics.builder()
            .fileInfos(
                Collections.singletonList(
                    FileInfo.builder()
                        .fileName("somename.txt")
                        .fileType(DwcFileType.CORE)
                        .rowType(DwcTerm.Occurrence.qualifiedName())
                        .terms(
                            Collections.singletonList(
                                TermInfo.builder()
                                    .term(DwcTerm.occurrenceStatus.qualifiedName())
                                    .interpretedIndexed(1L)
                                    .rawIndexed(1L)
                                    .build()))
                        .build()))
            .build();
    validation.setMetrics(metrics);
    validationWsClient.update(validation);
    Validation persistedValidation = validationWsClient.get(validation.getKey());
    assertEquals(metrics, persistedValidation.getMetrics());
    assertEquals(Validation.Status.FINISHED, persistedValidation.getStatus());
  }

  @Test
  public void cancelValidationIT() throws Exception {
    File archive = readTestFileInputStream("/archive.zip");
    Validation validation = validationWsClient.submitFile(archive);
    assertNotNull(validation);

    // Can the new validation be retrieved?
    Validation persistedValidation = validationWsClient.get(validation.getKey());
    assertNotNull(persistedValidation);

    validationWsClient.cancel(persistedValidation.getKey());

    // Wait for the submit operation to complete
    TimeUnit.SECONDS.sleep(2L);

    PagingResponse<Validation> validations =
        validationWsClient.list(
            ValidationSearchRequest.builder()
                .offset(0L)
                .limit(10)
                .status(Validation.finishedStatuses())
                .build());
    assertTrue(
        validations.getResults().stream()
            .anyMatch(v -> v.getKey().equals(persistedValidation.getKey())));
  }

  @Test
  public void deleteValidationIT() throws Exception {
    File archive = readTestFileInputStream("/archive.zip");
    Validation validation = validationWsClient.submitFile(archive);
    assertNotNull(validation);

    // Can the new validation be retrieved?
    Validation persistedValidation = validationWsClient.get(validation.getKey());
    assertNotNull(persistedValidation);

    // Wait for internal async task
    TimeUnit.SECONDS.sleep(3L);

    // Delete the validation
    validationWsClient.delete(persistedValidation.getKey());

    // Can get the validation but it is deleted
    Validation deletedValidation = validationWsClient.get(persistedValidation.getKey());
    assertNotNull(deletedValidation.getDeleted());

    // Validation is not in the results
    PagingResponse<Validation> validations =
        validationWsClient.list(
            ValidationSearchRequest.builder()
                .offset(0L)
                .limit(50)
                .sortByCreated(ValidationSearchRequest.SortOrder.DESC)
                .build());
    assertTrue(
        validations.getResults().stream()
            .noneMatch(v -> v.getKey().equals(persistedValidation.getKey())));
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
