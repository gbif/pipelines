package org.gbif.pipelines.core.interpreters.metadata;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.model.registry.Network;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.util.comparators.EndpointPriorityComparator;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.TagName;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.io.avro.MetadataRecord;

/** Interprets GBIF metadata by datasetId */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MetadataInterpreter {

  /** Gets information from GBIF API by datasetId */
  public static BiConsumer<String, MetadataRecord> interpret(MetadataServiceClient client) {
    return (datasetId, mdr) -> {

      // Set required metadata properties
      mdr.setDatasetKey(datasetId);
      mdr.setNetworkKeys(Collections.emptyList());

      if (client != null) {

        Dataset dataset = client.getDataset(datasetId);

        // https://github.com/gbif/pipelines/issues/401
        License license = dataset.getLicense();
        if (license == null || license == License.UNSPECIFIED || license == License.UNSUPPORTED) {
          throw new IllegalArgumentException(
              "Dataset licence can't be UNSPECIFIED or UNSUPPORTED!");
        } else {
          mdr.setLicense(license.name());
        }

        mdr.setDatasetTitle(dataset.getTitle());
        mdr.setInstallationKey(dataset.getInstallationKey().toString());
        Optional.ofNullable(dataset.getPublishingOrganizationKey())
            .map(UUID::toString)
            .ifPresent(mdr::setPublishingOrganizationKey);

        List<Network> networkList = client.getNetworkFromDataset(datasetId);
        if (networkList != null && !networkList.isEmpty()) {
          mdr.setNetworkKeys(
              networkList.stream()
                  .filter(x -> x.getKey() != null)
                  .map(n -> n.getKey().toString())
                  .collect(Collectors.toList()));
        }

        Organization organization = client.getOrganization(dataset.getPublishingOrganizationKey());
        Optional.ofNullable(organization.getEndorsingNodeKey())
            .map(UUID::toString)
            .ifPresent(mdr::setEndorsingNodeKey);
        mdr.setPublisherTitle(organization.getTitle());
        Optional.ofNullable(organization.getCountry())
            .map(Country::getIso2LetterCode)
            .ifPresent(mdr::setDatasetPublishingCountry);
        getLastCrawledDate(dataset.getMachineTags())
            .ifPresent(d -> mdr.setLastCrawled(d.getTime()));
        if (Objects.nonNull(dataset.getProject())) {
          mdr.setProjectId(dataset.getProject().getIdentifier());
          client
              .getDatasetProject(dataset)
              .ifPresent(p -> mdr.setProgrammeAcronym(p.getProgramme().getAcronym()));
        }

        Installation installation = client.getInstallation(dataset.getInstallationKey());
        Optional.ofNullable(installation.getOrganizationKey())
            .map(UUID::toString)
            .ifPresent(mdr::setHostingOrganizationKey);

        List<Endpoint> endpoints = prioritySortEndpoints(installation.getEndpoints());
        if (!endpoints.isEmpty()) {
          mdr.setProtocol(endpoints.get(0).getType().name());
        }
        copyMachineTags(dataset.getMachineTags(), mdr);
      }
    };
  }

  // Copied from CrawlerCoordinatorServiceImpl
  private static List<Endpoint> prioritySortEndpoints(List<Endpoint> endpoints) {
    // Filter out all Endpoints that we can't crawl
    return endpoints.stream()
        .filter(endpoint -> EndpointPriorityComparator.PRIORITIES.contains(endpoint.getType()))
        .sorted(new EndpointPriorityComparator())
        .collect(Collectors.toList());
  }

  /** Sets attempt number as crawlId */
  public static Consumer<MetadataRecord> interpretCrawlId(Integer attempt) {
    return mdr -> Optional.ofNullable(attempt).ifPresent(mdr::setCrawlId);
  }

  /** Gets the latest crawl attempt time, if exists. */
  private static Optional<Date> getLastCrawledDate(List<MachineTag> machineTags) {
    return Optional.ofNullable(machineTags)
        .flatMap(
            x ->
                x.stream()
                    .filter(
                        tag ->
                            TagName.CRAWL_ATTEMPT.getName().equals(tag.getName())
                                && TagName.CRAWL_ATTEMPT
                                    .getNamespace()
                                    .getNamespace()
                                    .equals(tag.getNamespace()))
                    .sorted(Comparator.comparing(MachineTag::getCreated).reversed())
                    .map(MachineTag::getCreated)
                    .findFirst());
  }

  /** Copy MachineTags into the Avro Metadata record. */
  private static void copyMachineTags(List<MachineTag> machineTags, MetadataRecord mdr) {
    if (Objects.nonNull(machineTags) && !machineTags.isEmpty()) {
      mdr.setMachineTags(
          machineTags.stream()
              .map(
                  machineTag ->
                      org.gbif.pipelines.io.avro.MachineTag.newBuilder()
                          .setNamespace(machineTag.getNamespace())
                          .setName(machineTag.getName())
                          .setValue(machineTag.getValue())
                          .build())
              .collect(Collectors.toList()));
    }
  }
}
