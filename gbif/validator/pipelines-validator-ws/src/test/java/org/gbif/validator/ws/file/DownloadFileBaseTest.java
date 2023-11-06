package org.gbif.validator.ws.file;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.HttpStatusCode;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/** {@link DownloadFileManager} tests. */
public abstract class DownloadFileBaseTest {

  /** Test configuration, only for this test and enable async methods. */
  @Configuration
  @EnableAsync
  public static class DownloadFileManagerTestConfiguration {

    @Bean
    public DownloadFileManager downloadFileManager() {
      return new DownloadFileManager();
    }
  }

  // Temporary directory in where to store the files
  @TempDir static Path tempDir;

  // Mock http file server
  protected final ClientAndServer clientAndServer;

  // This Spring context is needed because the DownloadFileManager has  method annotated with @Async
  protected static AnnotationConfigApplicationContext ctx;

  public DownloadFileBaseTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;
    setExpectations();
  }

  /** Set expected responses for the ClientAndServer mock server. */
  private void setExpectations() {
    clientAndServer
        .when(HttpRequest.request().withMethod("GET").withPath("/Archive.zip"))
        .respond(HttpResponse.response().withBody(readTestFile("/dwca/Archive.zip")));

    // HEAD requests are used to check if a file is available
    clientAndServer
        .when(HttpRequest.request().withMethod("HEAD").withPath("/Archive.zip"))
        .respond(HttpResponse.response().withStatusCode(HttpStatusCode.OK_200.code()));
  }

  @BeforeAll
  public static void init() {
    ctx = new AnnotationConfigApplicationContext();
    ctx.register(DownloadFileManagerTestConfiguration.class);
    ctx.refresh();
    ctx.start();
  }

  @AfterAll
  public static void tearDown() {
    if (ctx != null && ctx.isRunning()) {
      ctx.stop();
    }
  }

  @SneakyThrows
  protected static byte[] readTestFile(String file) {
    return Files.readAllBytes(Paths.get(DownloadFileBaseTest.class.getResource(file).getFile()));
  }

  @SneakyThrows
  protected static InputStream readTestFileInputStream(String file) {
    return new FileInputStream(DownloadFileBaseTest.class.getResource(file).getFile());
  }

  /** Creates a path to the local MockServer url. */
  protected String testPath(String path) {
    return "http://127.0.0.1:" + clientAndServer.getPort() + path;
  }
}
