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

  public static BiConsumer<String, MetadataRecord> interpret(WsConfig wsConfig) {
    return (datasetId, mdr) -> {
      MetadataServiceClient client = MetadataServiceClient.create(wsConfig);

      Dataset dataset = client.getDataset(datasetId);
      mdr.setInstallationKey(dataset.getInstallationKey());
      mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
      mdr.setLicense(dataset.getLicense());

      Installation installation = client.getInstallation(mdr.getInstallationKey());
      mdr.setOrganizationKey(installation.getOrganizationKey());
      mdr.setProtocol(installation.getProtocol());

      Organization organization = client.getOrganization(mdr.getOrganizationKey());
      mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
    };
  }
}
