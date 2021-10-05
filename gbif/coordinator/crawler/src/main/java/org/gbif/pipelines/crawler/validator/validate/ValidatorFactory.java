package org.gbif.pipelines.crawler.validator.validate;

import lombok.Builder;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.crawler.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.ws.client.ValidationWsClient;

@Builder
public class ValidatorFactory {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final PipelinesArchiveValidatorMessage message;

  public ArchiveValidator create() {

    // DWCA
    if (message.getFileFormat().equals(FileFormat.DWCA.name())) {
      return DwcaArchiveValidator.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .schemaValidatorFactory(schemaValidatorFactory)
          .build();
    }

    // XML
    if (message.getFileFormat().equals(FileFormat.XML.name())) {
      return XmlArchiveValidator.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .schemaValidatorFactory(schemaValidatorFactory)
          .build();
    }

    // Defualt
    return FailedValidator.builder().validationClient(validationClient).message(message).build();
  }
}
