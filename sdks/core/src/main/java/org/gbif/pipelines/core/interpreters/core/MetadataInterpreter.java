package org.gbif.pipelines.core.interpreters.core;

import java.util.function.BiConsumer;

import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Installation;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Organization;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Interprets GBIF metadata by datasetId */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MetadataInterpreter {

  /**
   * Gets information from GBIF API by datasetId
   */
  public static BiConsumer<String, MetadataRecord> interpret(MetadataServiceClient client) {
    return (datasetId, mdr) -> {
      mdr.setDatasetKey(datasetId);

      Dataset dataset = client.getDataset(datasetId);
      mdr.setDatasetTitle(dataset.getTitle());
      mdr.setInstallationKey(dataset.getInstallationKey());
      mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
      mdr.setLicense(dataset.getLicense());

      Installation installation = client.getInstallation(mdr.getInstallationKey());
      mdr.setOrganizationKey(installation.getOrganizationKey());
      mdr.setProtocol(installation.getProtocol());

      Organization organization = client.getOrganization(mdr.getOrganizationKey());
      mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
      mdr.setPublisherTitle(organization.getTitle());
    };
  }
}
