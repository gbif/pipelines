package org.gbif.dwca.validation.xml;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

@UtilityClass
class TestUtils {

  @SneakyThrows
  public static String readTestFile(String file) {
    return new String(
        Files.readAllBytes(Paths.get(TestUtils.class.getResource(file).getFile())),
        StandardCharsets.UTF_8);
  }

  /** Creates a path to the a local MockServer url. */
  public static String testPath(ClientAndServer clientAndServer, String path) {
    return "http://127.0.0.1:" + clientAndServer.getPort() + path;
  }

  @SneakyThrows
  public static void createEmlSchemasExpectation(ClientAndServer clientAndServer) {
    clientAndServer
        .when(HttpRequest.request().withMethod("GET").withPath("/eml.xsd"))
        .respond(HttpResponse.response(readTestFile("/schemas/eml.xsd")));

    clientAndServer
        .when(HttpRequest.request().withMethod("GET").withPath("/eml-gbif-profile.xsd"))
        .respond(HttpResponse.response(readTestFile("/schemas/eml-gbif-profile.xsd")));

    clientAndServer
        .when(HttpRequest.request().withMethod("GET").withPath("/dc.xsd"))
        .respond(HttpResponse.response(readTestFile("/schemas/dc.xsd")));
  }
}
