package org.gbif.pipelines.core.interpreters;

import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Installation;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Organization;
import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.util.function.BiConsumer;

/** Interprets GBIF metadata by datasetId */
public class MetadataInterpreter {

  private MetadataInterpreter() {}

  public static BiConsumer<String, MetadataRecord> interpretDataset(WsConfig wsConfig) {
    return (datasetId, mdr) -> {
      Dataset dataset = MetadataServiceClient.create(wsConfig).getDataset(datasetId);
      mdr.setInstallationKey(dataset.getInstallationKey());
      mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
      mdr.setLicense(dataset.getLicense());
    };
  }

  public static BiConsumer<String, MetadataRecord> interpretInstallation(WsConfig wsConfig) {
    return (datasetId, mdr) -> {
      Installation installation =
          MetadataServiceClient.create(wsConfig).getInstallation(mdr.getInstallationKey());
      mdr.setOrganizationKey(installation.getOrganizationKey());
      mdr.setProtocol(installation.getProtocol());
    };
  }

  public static BiConsumer<String, MetadataRecord> interpretOrganization(WsConfig wsConfig) {
    return (datasetId, mdr) -> {
      Organization organization =
          MetadataServiceClient.create(wsConfig).getOrganization(mdr.getOrganizationKey());
      mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
    };
  }
}
