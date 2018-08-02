package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.core.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.client.metadata.response.Installation;
import org.gbif.pipelines.core.ws.client.metadata.response.Organization;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.MetadataRecord;

import java.util.function.Function;

public interface MetadataInerpreter extends Function<String, Interpretation<String>> {

  static MetadataInerpreter interpretDataset(MetadataRecord metadataRecord, Config wsConfig) {
    return (String datasetId) -> {
      Interpretation<String> interpretation = Interpretation.of(datasetId);
      Dataset dataset = MetadataServiceClient.create(wsConfig).getDataset(datasetId);
      metadataRecord.setInstallationKey(dataset.getInstallationKey());
      metadataRecord.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
      return interpretation;
    };
  }

  static MetadataInerpreter interpretInstallation(MetadataRecord metadataRecord, Config wsConfig) {
    return (String datasetId) -> {
      Interpretation<String> interpretation = Interpretation.of(datasetId);
      Installation installation =
          MetadataServiceClient.create(wsConfig)
              .getInstallation(metadataRecord.getInstallationKey());
      metadataRecord.setOrganizationKey(installation.getOrganizationKey());
      return interpretation;
    };
  }

  static MetadataInerpreter interpretOrganization(MetadataRecord metadataRecord, Config wsConfig) {
    return (String datasetId) -> {
      Interpretation<String> interpretation = Interpretation.of(datasetId);
      Organization organization =
          MetadataServiceClient.create(wsConfig)
              .getOrganization(metadataRecord.getOrganizationKey());
      metadataRecord.setEndorsingNodeKey(organization.getEndorsingNodeKey());
      return interpretation;
    };
  }
}
