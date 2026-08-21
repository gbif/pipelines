package org.gbif.pipelines.tasks.validators.validator.validate;

import java.util.Optional;
import lombok.Builder;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.checklists.ChecklistValidator;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.ws.client.ValidationWsClient;

@Builder
public class ArchiveValidatorFactory {

  private final ArchiveValidatorConfiguration config;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;
  private final PipelinesArchiveValidatorMessage message;

  public ArchiveValidator create() {

    Optional<DatasetType> datasetTypeOpt =
        DatasetTypeUtils.getDatasetType(config.archiveRepository, message.getDatasetUuid());

    if (datasetTypeOpt.isPresent() && datasetTypeOpt.get() == DatasetType.CHECKLIST) {
      return ChecklistDwcaArchiveValidator.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .schemaValidatorFactory(schemaValidatorFactory)
          .checklistValidator(
              new ChecklistValidator(
                  config.clbConfig.url,
                  config.clbConfig.user,
                  config.clbConfig.password,
                  config.clbConfig.callbackUrl))
          .build();
    }

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

    // Default
    return DefaultValidator.builder().validationClient(validationClient).message(message).build();
  }
}
