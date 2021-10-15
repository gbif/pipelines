package org.gbif.dwca.validation.xml;

import static org.gbif.dwca.validation.xml.TestUtils.createEmlSchemasExpectation;
import static org.gbif.dwca.validation.xml.TestUtils.testPath;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import lombok.SneakyThrows;
import org.gbif.dwca.validation.XmlSchemaValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;

@ExtendWith(MockServerExtension.class)
public class SchemaValidatorFactoryTest {

  private final ClientAndServer clientAndServer;

  private final SchemaValidatorFactory schemaValidatorFactory;

  @SneakyThrows
  public SchemaValidatorFactoryTest(ClientAndServer clientAndServer) {
    this.clientAndServer = clientAndServer;

    // Create the test endpoints for eml.xsd a dependant schemas
    createEmlSchemasExpectation(clientAndServer);

    schemaValidatorFactory = new SchemaValidatorFactory(testPath(clientAndServer, "/eml.xsd"));
  }

  @Test
  public void testLoadSchema() {

    // Gets the test schema
    XmlSchemaValidator emlSchemaValidator =
        schemaValidatorFactory.newValidator(testPath(clientAndServer, "/eml.xsd"));

    // The schema loads successfully
    assertNotNull(emlSchemaValidator);
  }

  @Test
  public void testNotExistingSchema() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          // Gets the test schema
          schemaValidatorFactory.newValidator(testPath(clientAndServer, "/notExist.xsd"));
          // Should fail if next line is reached
          fail();
        });
  }
}
