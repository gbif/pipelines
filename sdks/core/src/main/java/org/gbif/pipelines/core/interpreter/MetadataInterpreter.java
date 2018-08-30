package org.gbif.pipelines.core.interpreter;

import org.gbif.pipelines.core.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.client.metadata.response.Installation;
import org.gbif.pipelines.core.ws.client.metadata.response.Organization;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.MetadataRecord;

import java.util.function.BiConsumer;

/** Interprets GBIF metadata by datasetId */
public interface MetadataInterpreter extends BiConsumer<String, MetadataRecord> {

  static MetadataInterpreter interpretDataset(Config wsConfig) {
    return (datasetId, mdr) -> {
      Dataset dataset = MetadataServiceClient.create(wsConfig).getDataset(datasetId);
      mdr.setInstallationKey(dataset.getInstallationKey());
      mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
      mdr.setLicense(dataset.getLicense());
    };
  }

  static MetadataInterpreter interpretInstallation(Config wsConfig) {
    return (datasetId, mdr) -> {
      Installation installation =
          MetadataServiceClient.create(wsConfig).getInstallation(mdr.getInstallationKey());
      mdr.setOrganizationKey(installation.getOrganizationKey());
      mdr.setProtocol(installation.getProtocol());
    };
  }

  static MetadataInterpreter interpretOrganization(Config wsConfig) {
    return (datasetId, mdr) -> {
      Organization organization =
          MetadataServiceClient.create(wsConfig).getOrganization(mdr.getOrganizationKey());
      mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
    };
  }
}
