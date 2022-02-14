package org.gbif.pipelines.tasks.validator.validate;

import lombok.Builder;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.ws.client.ValidationWsClient;

@Builder
public class ArchiveValidatorFactory {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final PipelinesArchiveValidatorMessage message;

  public ArchiveValidator create() {

    // DWCA
    if (FileFormat.DWCA.name().equals(message.getFileFormat())) {
      return DwcaArchiveValidator.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .schemaValidatorFactory(schemaValidatorFactory)
          .build();
    }

    // XML
    if (FileFormat.XML.name().equals(message.getFileFormat())) {
      return XmlArchiveValidator.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .build();
    }

    // Tabular or spreadsheet
    if (FileFormat.TABULAR.name().equals(message.getFileFormat())
        || FileFormat.SPREADSHEET.name().equals(message.getFileFormat())) {
      return SingleFileArchiveValidator.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .build();
    }

    // Defualt
    return DefaultValidator.builder().validationClient(validationClient).message(message).build();
  }
}
