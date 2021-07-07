package org.gbif.dwca.validation.xml;

import static org.gbif.dwca.validation.xml.TestUtils.createEmlSchemasExpectation;
import static org.gbif.dwca.validation.xml.TestUtils.readTestFile;
import static org.gbif.dwca.validation.xml.TestUtils.testPath;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.SneakyThrows;
import org.gbif.dwca.validation.XmlSchemaValidator;
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
    XmlSchemaValidator emlValidator = getEmlValidator();
    XmlSchemaValidator.ValidationResult result =
        emlValidator.validate(readTestFile("/xml/ebird-eml.xml"));
    assertTrue(result.isValid());
  }

  @Test
  public void invalidXmlTest() {
    XmlSchemaValidator emlValidator = getEmlValidator();
    XmlSchemaValidator.ValidationResult result =
        emlValidator.validate(readTestFile("/xml/invalid-ebird-eml.xml"));
    assertFalse(result.isValid());
    assertTrue(result.getErrors().size() > 0);
  }
}
