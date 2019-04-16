package org.gbif.pipelines.core.interpreters.core;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.TagName;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.parsers.ws.client.metadata.MetadataServiceClient;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Dataset;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Network;
import org.gbif.pipelines.parsers.ws.client.metadata.response.Organization;

import com.google.common.base.Strings;
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
          mdr.setNetworkKeys(networkList.stream().map(Network::getKey).collect(Collectors.toList()));
        } else {
          mdr.setNetworkKeys(Collections.emptyList());
        }

        Organization organization = client.getOrganization(dataset.getPublishingOrganizationKey());
        mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
        mdr.setPublisherTitle(organization.getTitle());
        mdr.setDatasetPublishingCountry(organization.getCountry());
        getLastCrawledDate(dataset.getMachineTags()).ifPresent(d -> mdr.setLastCrawled(d.getTime()));
      }
    };
  }

  public static Consumer<MetadataRecord> interpretEndointType(String endpointType) {
    return mdr -> {
      if (!Strings.isNullOrEmpty(endpointType)) {
        EndpointType ept = VocabularyUtils.lookup(endpointType, EndpointType.class).get();
        mdr.setProtocol(ept.name());
      }
    };
  }

  /**
   * Gets the latest crawl attempt time, if exists.
   */
  private static Optional<Date> getLastCrawledDate(List<MachineTag> machineTags) {
    if (Objects.nonNull(machineTags)) {
      return machineTags.stream()
          .filter(tag -> TagName.CRAWL_ATTEMPT.getName().equals(tag.getName())
              && TagName.CRAWL_ATTEMPT.getNamespace().getNamespace().equals(tag.getNamespace()))
          .sorted(Comparator.comparing(MachineTag::getCreated).reversed())
          .map(MachineTag::getCreated)
          .findFirst();
    }
    return Optional.empty();
  }
}
