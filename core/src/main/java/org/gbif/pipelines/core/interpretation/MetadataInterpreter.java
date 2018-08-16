package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.core.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.client.metadata.response.Installation;
import org.gbif.pipelines.core.ws.client.metadata.response.Organization;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.MetadataRecord;

import java.util.function.Function;

public interface MetadataInterpreter extends Function<String, Interpretation<String>> {


  static MetadataInterpreter interpretDataset(MetadataRecord metadataRecord, Config wsConfig) {
    return (String datasetId) -> {
      Interpretation<String> interpretation = Interpretation.of(datasetId);
      Dataset dataset = MetadataServiceClient.create(wsConfig).getDataset(datasetId);
      metadataRecord.setInstallationKey(dataset.getInstallationKey());
      metadataRecord.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
      metadataRecord.setLicense(dataset.getLicense());
      return interpretation;
    };
  }

  static MetadataInterpreter interpretInstallation(MetadataRecord metadataRecord, Config wsConfig) {
    return (String datasetId) -> {
      Interpretation<String> interpretation = Interpretation.of(datasetId);
      Installation installation =
          MetadataServiceClient.create(wsConfig)
              .getInstallation(metadataRecord.getInstallationKey());
      metadataRecord.setOrganizationKey(installation.getOrganizationKey());
      metadataRecord.setProtocol(installation.getProtocol());
      return interpretation;
    };
  }

  static MetadataInterpreter interpretOrganization(MetadataRecord metadataRecord, Config wsConfig) {
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
