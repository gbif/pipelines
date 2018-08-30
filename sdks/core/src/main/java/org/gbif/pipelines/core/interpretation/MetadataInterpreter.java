package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.core.Context;
import org.gbif.pipelines.core.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.client.metadata.response.Installation;
import org.gbif.pipelines.core.ws.client.metadata.response.Organization;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.io.avro.MetadataRecord;

import java.util.function.BiConsumer;

/** TODO */
public interface MetadataInterpreter extends BiConsumer<String, MetadataRecord> {

  static Context<String, MetadataRecord> createContext(String datasetId) {
    MetadataRecord mdr = MetadataRecord.newBuilder().setDatasetId(datasetId).build();
    return new Context<>(datasetId, mdr);
  }

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
