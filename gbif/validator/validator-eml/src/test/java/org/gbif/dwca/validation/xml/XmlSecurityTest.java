package org.gbif.dwca.validation.xml;

import static org.gbif.dwca.validation.xml.TestUtils.createEmlSchemasExpectation;
import static org.gbif.dwca.validation.xml.TestUtils.readTestFile;
import static org.gbif.dwca.validation.xml.TestUtils.testPath;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.gbif.validator.api.Level;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.HttpRequest;
import org.mockserver.verify.VerificationTimes;
import org.xml.sax.SAXParseException;

@ExtendWith(MockServerExtension.class)
class XmlSecurityTest {

  private static final String SECRET = "very-secret-value";

  private final ClientAndServer clientAndServer;

  XmlSecurityTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;
    createEmlSchemasExpectation(clientAndServer);
  }

  private static String withEntity(String eml, Path secret) {
    return eml.replaceFirst(
            "<eml:eml",
            "<!DOCTYPE eml:eml [<!ENTITY x SYSTEM \"" + secret.toUri() + "\">]>\n<eml:eml")
        .replaceFirst("<title>", "<title>&x;");
  }

  private static Map<String, String> relatedData(IssueInfo issue) {
    return issue.getSamples().get(0).getRelatedData();
  }

  @Test
  void externalEntityIsNotResolved(@TempDir Path dir) throws Exception {
    Path secret = Files.writeString(dir.resolve("secret.txt"), SECRET);
    String eml = withEntity(readTestFile("/xml/ebird-eml.xml"), secret);

    SchemaValidatorFactory factory =
        new SchemaValidatorFactory(
            Set.of(),
            testPath(clientAndServer, "/dc.xsd"),
            testPath(clientAndServer, "/eml-gbif-profile.xsd"),
            testPath(clientAndServer, "/eml.xsd"));
    List<IssueInfo> issues =
        factory.newValidator(testPath(clientAndServer, "/eml.xsd")).validate(eml);

    assertEquals(1, issues.size());
    assertTrue(relatedData(issues.get(0)).containsKey(Level.FATAL.name()));
    assertFalse(issues.toString().contains(SECRET));
  }

  @Test
  void documentSchemaLocationIsNotFetchedUnlessTrusted() {
    String eml =
        readTestFile("/xml/ebird-eml.xml")
            .replace(
                "http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd",
                testPath(clientAndServer, "/attacker.xsd"));

    List<IssueInfo> issues = new SchemaValidatorFactory().validate(eml);

    assertEquals(1, issues.size());
    clientAndServer.verify(
        HttpRequest.request().withPath("/attacker.xsd"), VerificationTimes.exactly(0));
  }

  @Test
  void onlyHttpSchemasOfTrustedHostsAreAllowed() {
    SchemaValidatorFactory factory = new SchemaValidatorFactory();
    assertTrue(factory.isAllowed("http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd"));
    assertTrue(factory.isAllowed("https://rs.gbif.org/schema/eml-gbif-profile/1.2/eml.xsd"));
    assertFalse(factory.isAllowed("http://169.254.169.254/latest/meta-data"));
    assertFalse(factory.isAllowed("file:///etc/passwd"));
    assertFalse(factory.isAllowed("jar:http://rs.gbif.org/x.jar!/eml.xsd"));
    assertFalse(factory.isAllowed("http://rs.gbif.org.attacker.example/eml.xsd"));
    assertFalse(factory.isAllowed(null));
    assertThrows(IllegalArgumentException.class, () -> factory.newValidator("file:///etc/passwd"));
  }

  @Test
  void assertNoDoctype(@TempDir Path dir) throws Exception {
    Path secret = Files.writeString(dir.resolve("secret.txt"), SECRET);
    String eml = readTestFile("/xml/ebird-eml.xml");

    assertDoesNotThrow(() -> XmlSecurity.assertNoDoctype(eml.getBytes(StandardCharsets.UTF_8)));
    assertThrows(
        SAXParseException.class,
        () ->
            XmlSecurity.assertNoDoctype(withEntity(eml, secret).getBytes(StandardCharsets.UTF_8)));
  }
}
