package org.gbif.pipelines.tasks.validators.validator.validate;

import lombok.Builder;
import org.gbif.common.messaging.api.messages.AbstractPipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.ws.client.ValidationWsClient;

@Builder
public class ArchiveValidatorFactory {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final AbstractPipelinesArchiveValidatorMessage message;
  private final DwcaArchiveValidatorOutgoingMessageCreator outgoingMessageCreator;

  public ArchiveValidator create() {

    // DWCA
    if (FileFormat.DWCA.name().equals(message.getFileFormat())) {
      return DwcaArchiveValidator.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .schemaValidatorFactory(schemaValidatorFactory)
          .outgoingMessageCreator(outgoingMessageCreator)
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

    // Default
    return DefaultValidator.builder().validationClient(validationClient).message(message).build();
  }
}
