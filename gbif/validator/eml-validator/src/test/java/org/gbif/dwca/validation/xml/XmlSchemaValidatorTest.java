package org.gbif.dwca.validation.xml;

import static org.gbif.dwca.validation.xml.TestUtils.createEmlSchemasExpectation;
import static org.gbif.dwca.validation.xml.TestUtils.readTestFile;
import static org.gbif.dwca.validation.xml.TestUtils.testPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import lombok.SneakyThrows;
import org.gbif.dwca.validation.XmlSchemaValidator;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;

/** XmlSchemaXmlValidator tests. */
@ExtendWith(MockServerExtension.class)
public class XmlSchemaValidatorTest {

  private final ClientAndServer clientAndServer;

  private final SchemaValidatorFactory schemaValidatorFactory;

  @SneakyThrows
  public XmlSchemaValidatorTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;

    // Create the test endpoints for eml.xsd a dependant schemas
    createEmlSchemasExpectation(clientAndServer);

    schemaValidatorFactory =
        new SchemaValidatorFactory(
            testPath(clientAndServer, "/dc.xsd"),
            testPath(clientAndServer, "/eml-gbif-profile.xsd"),
            testPath(clientAndServer, "/eml.xsd"));
  }

  private XmlSchemaValidator getEmlValidator() {
    return schemaValidatorFactory.newValidator(testPath(clientAndServer, "/eml.xsd"));
  }

  @Test
  public void validXmlTest() {
    // State
    XmlSchemaValidator emlValidator = getEmlValidator();
    String file = readTestFile("/xml/ebird-eml.xml");

    // When
    List<IssueInfo> result = emlValidator.validate(file);

    // Should
    assertTrue(result.isEmpty());
  }

  @Test
  public void invalidXmlTest() {
    // State
    XmlSchemaValidator emlValidator = getEmlValidator();
    String file = readTestFile("/xml/invalid-ebird-eml.xml");

    // When
    List<IssueInfo> result = emlValidator.validate(file);

    // Should
    assertEquals(1, result.size());

    IssueInfo info = result.get(0);
    assertEquals(EvaluationType.EML_GBIF_SCHEMA.name(), info.getIssue());
  }
}
