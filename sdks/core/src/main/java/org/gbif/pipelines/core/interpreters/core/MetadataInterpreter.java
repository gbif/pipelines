package org.gbif.pipelines.core.interpreters.core;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Installation;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Network;
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
      if (client != null) {

        mdr.setDatasetKey(datasetId);

        Dataset dataset = client.getDataset(datasetId);
        mdr.setDatasetTitle(dataset.getTitle());
        mdr.setInstallationKey(dataset.getInstallationKey());
        mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());
        mdr.setLicense(dataset.getLicense());

        List<Network> networkList = client.getNetworkFromDataset(datasetId);
        if (Objects.nonNull(networkList) && !networkList.isEmpty()) {
          mdr.setNetworkKeys(
              networkList.stream().map(Network::getKey).collect(Collectors.toList()));
        } else {
          mdr.setNetworkKeys(Collections.emptyList());
        }

        Installation installation = client.getInstallation(mdr.getInstallationKey());
        mdr.setOrganizationKey(installation.getOrganizationKey());
        mdr.setProtocol(installation.getType());

        Organization organization = client.getOrganization(mdr.getOrganizationKey());
        mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
        mdr.setPublisherTitle(organization.getTitle());
        mdr.setPublishingCountry(organization.getCountry());
      }
    };
  }
}
