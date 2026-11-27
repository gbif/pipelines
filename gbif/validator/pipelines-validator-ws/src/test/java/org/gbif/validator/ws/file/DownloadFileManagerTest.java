package org.gbif.validator.ws.file;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/** {@link DownloadFileManager} tests. */
@ExtendWith(MockServerExtension.class)
class DownloadFileManagerTest extends DownloadFileBaseTest {

  public DownloadFileManagerTest(ClientAndServer clientAndServer) {
    super(clientAndServer);
  }

  @Test
  void isAvailableTest() {
    // Non-existing url
    assertFalse(
        ctx.getBean(DownloadFileManager.class).isAvailable(testPath("/ThisDoesNotExist.zip")));
  }

  /** Spring creates the bean through component scanning, as the application does. */
  @Test
  void springCreatesBeanTest() {
    try (AnnotationConfigApplicationContext context =
        new AnnotationConfigApplicationContext(DownloadFileManager.class)) {
      assertTrue(context.getBean(DownloadFileManager.class) != null);
    }
  }

  @Test
  @SneakyThrows
  void isPublicTest() {
    for (String address :
        new String[] {
          "127.0.0.1",
          "10.1.2.3",
          "172.16.0.1",
          "192.168.1.1",
          "169.254.169.254",
          "0.0.0.0",
          "100.64.0.1",
          "224.0.0.1",
          "255.255.255.255",
          "::1",
          "::",
          "fe80::1",
          "fd00::1",
          "::127.0.0.1",
          "64:ff9b::a9fe:a9fe",
          "::ffff:10.0.0.1",
          "2002:a9fe:a9fe::1",
          "2002:7f00:1::",
          "2001:0:4136:e378:8000:63bf:3fff:fdd2",
          "2001:db8::1"
        }) {
      assertFalse(DownloadFileManager.isPublic(InetAddress.getByName(address)), address);
    }
    for (String address :
        new String[] {"8.8.8.8", "130.225.0.1", "2001:4860:4860::8888", "2002:808:808::1"}) {
      assertTrue(DownloadFileManager.isPublic(InetAddress.getByName(address)), address);
    }
  }

  @Test
  void checkUrlTest() {
    DownloadFileManager downloadFileManager = new DownloadFileManager(new String[0]);
    for (String url :
        new String[] {
          "http://127.0.0.1/a.zip",
          "http://localhost:8080/actuator",
          "http://169.254.169.254/latest/meta-data/",
          "https://[::1]/a.zip",
          "file:///etc/passwd",
          "ftp://ftp.gbif.org/a.zip"
        }) {
      assertThrows(
          DownloadFileManager.UnsafeUrlException.class,
          () -> downloadFileManager.checkUrl(new URL(url)),
          url);
    }
  }

  @Test
  @SneakyThrows
  void connectsToResolvedAddressTest() {
    // files.test resolves to the MockServer, connections must use this resolution
    DownloadFileManager downloadFileManager =
        new DownloadFileManager(
            new String[] {"files.test"},
            host -> new InetAddress[] {InetAddress.getByName("127.0.0.1")});
    Path targetFilePath = tempDir.resolve("resolved.zip");

    downloadFileManager.download(
        "http://files.test:" + clientAndServer.getPort() + "/Archive.zip", targetFilePath);

    assertTrue(Files.exists(targetFilePath));
  }

  @Test
  void dnsRebindingTest() {
    // The first resolution, when checking the URL, is public, later ones point to the MockServer
    AtomicInteger resolutions = new AtomicInteger();
    DownloadFileManager downloadFileManager =
        new DownloadFileManager(
            new String[0],
            host ->
                new InetAddress[] {
                  InetAddress.getByName(
                      resolutions.getAndIncrement() == 0 ? "8.8.8.8" : "127.0.0.1")
                });
    String url = "http://rebind.test:" + clientAndServer.getPort() + "/rebind.zip";

    assertFalse(downloadFileManager.isAvailable(url));
    clientAndServer.verify(
        HttpRequest.request().withPath("/rebind.zip"), VerificationTimes.never());
  }

  @Test
  void redirectToPrivateAddressTest() {
    clientAndServer
        .when(HttpRequest.request().withPath("/redirect.zip"))
        .respond(
            HttpResponse.response()
                .withStatusCode(302)
                .withHeader("Location", "http://169.254.169.254/latest/meta-data/"));

    // not the Spring bean, its proxy wraps the exception
    DownloadFileManager downloadFileManager = new DownloadFileManager(new String[] {"127.0.0.1"});
    assertFalse(downloadFileManager.isAvailable(testPath("/redirect.zip")));
    assertThrows(
        DownloadFileManager.UnsafeUrlException.class,
        () -> downloadFileManager.download(testPath("/redirect.zip"), tempDir.resolve("r.zip")));
  }

  /** Synchronous download test. */
  @Test
  void downloadTest() {
    Path targetFilePath = tempDir.resolve("Archive.zip");

    DownloadFileManager downloadFileManager = ctx.getBean(DownloadFileManager.class);
    downloadFileManager.download(testPath("/Archive.zip"), targetFilePath);

    assertTrue(Files.exists(targetFilePath));
  }

  /** Files larger than the maximum size are not downloaded. */
  @Test
  void downloadTooLargeTest() {
    Path targetFilePath = tempDir.resolve("TooLarge.zip");
    DownloadFileManager downloadFileManager =
        new DownloadFileManager(new String[] {"127.0.0.1"}, InetAddress::getAllByName, 100);

    assertThrows(
        FileSizeException.class,
        () -> downloadFileManager.download(testPath("/Archive.zip"), targetFilePath));
    assertFalse(Files.exists(targetFilePath));
  }

  /** Asynchronous download test. */
  @Test
  @SneakyThrows
  void downloadAsyncTest() {
    Path targetFilePath = tempDir.resolve("Archive.zip");

    DownloadFileManager downloadFileManager = ctx.getBean(DownloadFileManager.class);
    CompletableFuture<File> fileDownload =
        downloadFileManager.downloadAsync(
            testPath("/Archive.zip"),
            targetFilePath,
            file -> assertTrue(Files.exists(targetFilePath)),
            err -> fail());

    assertTrue(Files.exists(fileDownload.get().toPath()));
  }
}
