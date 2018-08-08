package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.core.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.client.metadata.response.Installation;
import org.gbif.pipelines.core.ws.client.metadata.response.Organization;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.MetadataRecord;

import java.util.function.BiConsumer;

public interface MetadataInterpreter extends BiConsumer<String, Interpretation<MetadataRecord>> {

  static MetadataInterpreter interpretId() {
    return (datasetId, interpretation) -> interpretation.getValue().setDatasetId(datasetId);
  }

  static MetadataInterpreter interpretDataset(Config wsConfig) {
    return (datasetId, interpretation) -> {
      MetadataRecord metadataRecord = interpretation.getValue();
      Dataset dataset = MetadataServiceClient.create(wsConfig).getDataset(datasetId);
      metadataRecord.setInstallationKey(dataset.getInstallationKey());
      metadataRecord.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
    };
  }

  static MetadataInterpreter interpretInstallation(Config wsConfig) {
    return (datasetId, interpretation) -> {
      MetadataRecord metadataRecord = interpretation.getValue();
      Installation installation =
          MetadataServiceClient.create(wsConfig)
              .getInstallation(metadataRecord.getInstallationKey());
      metadataRecord.setOrganizationKey(installation.getOrganizationKey());
    };
  }

  static MetadataInterpreter interpretOrganization(Config wsConfig) {
    return (datasetId, interpretation) -> {
      MetadataRecord metadataRecord = interpretation.getValue();
      Organization organization =
          MetadataServiceClient.create(wsConfig)
              .getOrganization(metadataRecord.getOrganizationKey());
      metadataRecord.setEndorsingNodeKey(organization.getEndorsingNodeKey());
    };
  }
}
